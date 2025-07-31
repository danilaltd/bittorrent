BLOCK_SIZE = 16384
from math import ceil
from enum import Enum
from rwlock import RWLock
import time
import threading
import queue
from peer import Peer
from typing import Iterator

bl = int.bit_length

class Status(Enum):
    EMPTY = 0
    REQUESTED = 1
    DOWNLOADED = 2

class Piece:
    def __init__(self, piece_index: int, piece_size: int, ready_queue: queue.Queue[tuple[int, list[bytes]]], downloaded: bool):
        self._piece_index: int = piece_index
        self._piece_size: int = piece_size
        self.number_of_blocks = (piece_size + BLOCK_SIZE - 1) // BLOCK_SIZE
        
        self._blocks_empty = 0 if downloaded else (1 << self.number_of_blocks) - 1
        self._blocks_empty_lock = threading.Lock()
        
        self._blocks_requested = 0
        self._blocks_requested_lock = threading.Lock()
        
        self._block_states = [Status.DOWNLOADED] * self.number_of_blocks if downloaded else [Status.EMPTY] * self.number_of_blocks

        self._block_states_lock = RWLock("_block_states_lock")
        self._block_datas: list[bytes] = [b''] * self.number_of_blocks
        self._block_datas_lock = RWLock("_block_datas_lock")
        self._block_last_requested: list[float] = [time.time() for _ in range(self.number_of_blocks)]
        self._block_last_requested_lock = RWLock("_block_last_requested_lock")
        self._block_requested_by: list[set[Peer]] = [[] for _ in range(self.number_of_blocks)]
        self._block_requested_by_lock = RWLock("_block_requested_by_lock")
        
        self._ready_queue = ready_queue
        self._cur_status: Status = self._state
        self._set_status_lock = threading.Lock()

    def _set_empty(self, i):
        with self._blocks_empty_lock:
            self._blocks_empty |= (1 << i)

    def _reset_empty(self, i):
        with self._blocks_empty_lock:
            self._blocks_empty &= ~(1 << i)
        
    def _set_requested(self, i):
        with self._blocks_requested_lock:
            self._blocks_requested |= (1 << i)
            
    def _reset_requested(self, i):
        with self._blocks_requested_lock:
            self._blocks_requested &= ~(1 << i)
    
    @property
    def _state(self) -> Status:
        with self._block_states_lock.read_access:
            # states = list(self._block_states)
            if all(s == Status.DOWNLOADED for s in self._block_states):
                return Status.DOWNLOADED
            if any(s == Status.REQUESTED for s in self._block_states) or (Status.DOWNLOADED in self._block_states and Status.EMPTY in self._block_states):
                return Status.REQUESTED
        return Status.EMPTY

    def get_blocks_to_reset(self, timeout) -> list[int]:
        res = []
        if self._cur_status == Status.REQUESTED:
            cur_time = time.time()
            with self._block_last_requested_lock.read_access:
                for i in self.get_requested_blocks():
                    if cur_time - self._block_last_requested[i] > timeout:
                        res.append(i)
        return res
                        
            #  if (state == Status.DOWNLOADED and self._block_states[block_index] != state == Status.REQUESTED) or (state == Status.REQUESTED and self._block_states[block_index] != state == Status.EMPTY):
            # print(f"{self._block_states[block_index]} to {state}")             
    def set_block_state(self, block_index: int, state: Status, data: bytes = None, last_requested = None, peer: Peer = None, skip_notify: bool = False):
        with self._set_status_lock:
            prev_state = self._cur_status
            with self._block_states_lock.write_access:
                if state == Status.DOWNLOADED or self._block_states[block_index] != Status.DOWNLOADED:
                    self._block_states[block_index] = state
                    cancel = False
                else:
                    cancel = True
                    
            if not cancel:
                self._cur_status = self._state
                cur_state = self._cur_status
                
                if state == Status.EMPTY:
                    self._set_empty(block_index)
                    self._reset_requested(block_index)
                elif state == Status.REQUESTED:
                    self._set_requested(block_index)
                    self._reset_empty(block_index)
                else:
                    self._reset_empty(block_index)
                    self._reset_requested(block_index)
                if data:
                    with self._block_datas_lock.write_access:
                        self._block_datas[block_index] = data
                if last_requested:
                    with self._block_last_requested_lock.write_access:
                        self._block_last_requested[block_index] = last_requested
                    
                with self._block_requested_by_lock.write_access:
                    requested_by = self._block_requested_by[block_index]
                    if state == Status.EMPTY:
                        for prev_peer in requested_by:
                            prev_peer.cancel_block(self._piece_index, block_index, skip_notify)
                        requested_by.clear()
                        if peer:
                            requested_by.append(peer)
                    elif state == Status.DOWNLOADED:
                        for prev_peer in requested_by:
                            if prev_peer == peer:
                                peer.got_block(self._piece_index, block_index)
                            else:
                                prev_peer.cancel_block(self._piece_index, block_index, skip_notify)
                        requested_by.clear()
                        if peer:
                            requested_by.append(peer)
                    elif state == Status.REQUESTED:
                        if peer and peer not in requested_by:
                            peer.request_block(self._piece_index, block_index)
                            requested_by.append(peer)
                            
                if state == Status.DOWNLOADED and self._cur_status == Status.DOWNLOADED:
                    self._ready_queue.put((self._piece_index, self._block_datas))
                    
                return prev_state, cur_state
        
            else:
                return prev_state, prev_state
        
    def get_empty_blocks(self) -> Iterator[int]:
        candidates = self._blocks_empty
        while candidates:
            lsb = candidates & -candidates
            yield bl(lsb) - 1
            candidates &= candidates - 1
            
    def get_requested_blocks(self) -> Iterator[int]:
        candidates = self._blocks_requested
        while candidates:
            lsb = candidates & -candidates
            yield bl(lsb) - 1
            candidates &= candidates - 1