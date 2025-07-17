BLOCK_SIZE = 16384
from math import ceil
from enum import Enum
from rwlock import RWLock
from rwlock import timed_lock
import time
import threading
import queue
import os
from datetime import datetime
from typing import Iterator

bl = int.bit_length

class Status(Enum):
    EMPTY = 0
    REQUESTED = 1
    DOWNLOADED = 2

def log_info(msg, flags = None, name = ''):
    if flags is None:
        flags = []
    flags.insert(0, f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_entry = f'{msg}\n'
    res = ''
    for flag in flags:
        res += f"[{flag}]"
    res += ' '    
    res += log_entry    
    if name:
        path = os.path.join('logs', 'peermanager')
    else:
        name = 'blockandpiece.log'
        path = 'logs'
        
    with open(os.path.join(f'{path}', f"{name}"), 'a', encoding='utf-8') as f:
        f.write(res)

class Block:
    def __init__(self, block_size = BLOCK_SIZE):
        self.block_size = block_size
        self._last_requested = None
        self._last_requested_lock = RWLock("_last_requested_lock")

    @property
    def last_requested(self):
        with timed_lock(self._last_requested_lock.read_access, "_last_requested_lock.read_access"):
            return self._last_requested
    @last_requested.setter
    def last_requested(self, value):
        with timed_lock(self._last_requested_lock.write_access, "_last_requested_lock.write_access"):
            self._last_requested = value
    
class Piece:
    def __init__(self, piece_index: int, piece_size: int, piece_sha1, ready_queue: queue.Queue[tuple[int, list[bytes]]]):
        self._piece_index: int = piece_index
        self.piece_size: int = piece_size
        self._piece_sha1 = piece_sha1
        self.number_of_blocks = ceil(self.piece_size / BLOCK_SIZE)
        self._blocks: list[Block] = []
        self._blocks_lock = RWLock("_blocks_lock")
        
        self._blocks_empty = (1 << self.number_of_blocks) - 1
        self._blocks_empty_lock = threading.Lock()
        
        self._init_blocks()
        
        self._block_states = [Status.EMPTY for _ in range(self.number_of_blocks)]
        self._block_states_lock = RWLock("_block_states_lock")
        self._block_datas: list[bytes] = [b'' for _ in range(self.number_of_blocks)]
        self._block_datas_lock = RWLock("_block_datas_lock")
        
        self._ready_queue = ready_queue
        self._cur_status: Status = Status.EMPTY

    def reset_empty(self, i):
        with timed_lock(self._blocks_empty_lock, "_blocks_empty_lock.write_access"):
            self._blocks_empty &= ~(1 << i)
        
    def set_empty(self, i):
        with timed_lock(self._blocks_empty_lock, "_blocks_empty_lock.write_access"):
            self._blocks_empty |= (1 << i)
        

    @property
    def state(self) -> Status:
        with timed_lock(self._block_states_lock.read_access, "_block_states_lock.read_access"):
            states = set(self._block_states)
        if states == {Status.DOWNLOADED}:
            return Status.DOWNLOADED
        if Status.REQUESTED in states or (Status.DOWNLOADED in states and Status.EMPTY in states):
            return Status.REQUESTED
        return Status.EMPTY

    def _init_blocks(self):
        pass
        count = max(1, self.number_of_blocks)
        remainder = self.piece_size % BLOCK_SIZE
        with timed_lock(self._blocks_lock.write_access, "_blocks_lock.write_access"):
            with timed_lock(self._blocks_empty_lock, "_blocks_empty_lock.write_access"):
                for i in range(self.number_of_blocks):
                    block_size = remainder if (i == count - 1 and remainder) else BLOCK_SIZE
                    self._blocks.append(Block(block_size))

        
        
    def monitor_block_timeouts(self, timeout) -> Status:
        with timed_lock(self._blocks_lock.read_access, "_blocks_lock.read_access"):
            blocks_copy = self._blocks.copy()
        for block_index, block in enumerate(blocks_copy):
            # if block.status == Status.REQUESTED and block.last_requested:
                if time.time() - block.last_requested > timeout:
                    real_status = block.changeStatusForce(Status.EMPTY)
                    block.last_requested = None
                    # self._fix_sets(real_status, block_index)    
                # else:
                    # print("not time.time() - block.last_requested > timeout:")     
            # else:
                # print("not block.status == Status.REQUESTED and block.last_requested")        

        # return self.cur_status
    
    def set_block_state(self, block_index: int, state: Status, data: bytes, last_requested):
        prev_state = self._cur_status
        with timed_lock(self._block_states_lock.write_access, "_block_states_lock.write_access"):
            #  if (state == Status.DOWNLOADED and self._block_states[block_index] != state == Status.REQUESTED) or (state == Status.REQUESTED and self._block_states[block_index] != state == Status.EMPTY):
            # print(f"{self._block_states[block_index]} to {state}")
            self._block_states[block_index] = state
        self._cur_status = self.state
        if state != Status.EMPTY:
            self.reset_empty(block_index)
            
        with timed_lock(self._block_datas_lock.write_access, "_block_datas_lock.write_access"):
            self._block_datas[block_index] = data
        # with timed_lock(self._blocks_lock.read_access, "_blocks_lock.read_access"):
            # self._blocks[block_index].last_requested = last_requested
            
        if state == Status.DOWNLOADED and self._cur_status == Status.DOWNLOADED:
            self._ready_queue.put((self._piece_index, self._block_datas))
            
        return prev_state, self._cur_status
        
                    
    def is_complete(self) -> bool:
        return self._cur_status == Status.DOWNLOADED

    def is_requested(self) -> bool:
        return self._cur_status == Status.REQUESTED

    def is_empty(self) -> bool:
        return self._cur_status == Status.EMPTY

    def is_block_empty(self, block_index: int) -> bool:
        return self._block_states[block_index] == Status.EMPTY

    def get_empty_blocks(self) -> Iterator[int]:
        candidates = self._blocks_empty
        while candidates:
            lsb = candidates & -candidates
            yield bl(lsb) - 1
            candidates &= candidates - 1