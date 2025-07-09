BLOCK_SIZE = 16384
from math import ceil
from enum import Enum
from rwlock import RWLock
from rwlock import timed_lock
import time

class Status(Enum):
    EMPTY = 0
    REQUESTED = 1
    RECEIVED = 2

class Block:
    def __init__(self, block_size = BLOCK_SIZE, raw_bytes = b""):
        self._block_size = block_size
        self._block_size_lock = RWLock("_block_size_lock")
        self._data = raw_bytes
        self._data_lock = RWLock("_data_lock")
        self._status = Status.EMPTY
        self._status_lock = RWLock("_status_lock")
        self._last_requested = None
        self._last_requested_lock = RWLock("_last_requested_lock")

    @property
    def block_size(self):
        with timed_lock(self._block_size_lock.read_access, "_block_size_lock.read_access"):
            return self._block_size
    @block_size.setter
    def block_size(self, value):
        with timed_lock(self._block_size_lock.write_access, "_block_size_lock.write_access"):
            self._block_size = value

    @property
    def data(self):
        with timed_lock(self._data_lock.read_access, "_data_lock.read_access"):
            return self._data
    @data.setter
    def data(self, value):
        with timed_lock(self._data_lock.write_access, "_data_lock.write_access"):
            self._data = value

    @property
    def status(self):
        with timed_lock(self._status_lock.read_access, "_status_lock.read_access"):
            return self._status
    @status.setter
    def status(self, value):
        with timed_lock(self._status_lock.write_access, "_status_lock.write_access"):
            self._status = value

    @property
    def last_requested(self):
        with timed_lock(self._last_requested_lock.read_access, "_last_requested_lock.read_access"):
            return self._last_requested
    @last_requested.setter
    def last_requested(self, value):
        with timed_lock(self._last_requested_lock.write_access, "_last_requested_lock.write_access"):
            self._last_requested = value
    
    def changeStatus(self, status: Status):
        if status == Status.EMPTY:
            pass
        elif status == Status.REQUESTED:
            if self.status != Status.RECEIVED:
                self.status = Status.REQUESTED
        elif status == Status.RECEIVED:
            self.status = Status.RECEIVED
        return self.status
    
    def changeStatusForce(self, status: Status):
        if status == Status.EMPTY:
            if self.status != Status.RECEIVED:
                self.status = Status.EMPTY
        elif status == Status.REQUESTED:
            if self.status != Status.RECEIVED:
                self.status = Status.REQUESTED
        elif status == Status.RECEIVED:
            self.status = Status.RECEIVED
        return self.status
class Piece:
    def __init__(self, piece_index, piece_size, piece_sha1):
        self._piece_index = piece_index
        self._piece_index_lock = RWLock("_piece_index_lock")
        self._piece_size = piece_size
        self._piece_size_lock = RWLock("_piece_size_lock")
        self._piece_sha1 = piece_sha1
        self._piece_sha1_lock = RWLock("_piece_sha1_lock")
        self._number_of_blocks = ceil(self._piece_size / BLOCK_SIZE)
        self._number_of_blocks_lock = RWLock("_number_of_blocks_lock")
        self._blocks: list[Block] = []
        self._blocks_lock = RWLock("_blocks_lock")
        self._downloaded_blocks: set[int] = set()
        self._downloaded_blocks_lock = RWLock("_downloaded_blocks_lock")
        self._requested_blocks: set[int] = set()
        self._requested_blocks_lock = RWLock("_requested_blocks_lock")
        self._empty_blocks: set[int] = set()
        self._empty_blocks_lock = RWLock("_empty_blocks_lock")
        self._cur_status: Status = Status.EMPTY
        self._cur_status_lock = RWLock("_cur_status_lock")
        self._init_blocks()

    @property
    def piece_index(self):
        with timed_lock(self._piece_index_lock.read_access, "_piece_index_lock.read_access"):
            return self._piece_index
    @piece_index.setter
    def piece_index(self, value):
        with timed_lock(self._piece_index_lock.write_access, "_piece_index_lock.write_access"):
            self._piece_index = value

    @property
    def piece_size(self):
        with timed_lock(self._piece_size_lock.read_access, "_piece_size_lock.read_access"):
            return self._piece_size
    @piece_size.setter
    def piece_size(self, value):
        with timed_lock(self._piece_size_lock.write_access, "_piece_size_lock.write_access"):
            self._piece_size = value

    @property
    def piece_sha1(self):
        with timed_lock(self._piece_sha1_lock.read_access, "_piece_sha1_lock.read_access"):
            return self._piece_sha1
    @piece_sha1.setter
    def piece_sha1(self, value):
        with timed_lock(self._piece_sha1_lock.write_access, "_piece_sha1_lock.write_access"):
            self._piece_sha1 = value

    @property
    def number_of_blocks(self):
        with timed_lock(self._number_of_blocks_lock.read_access, "_number_of_blocks_lock.read_access"):
            return self._number_of_blocks
    @number_of_blocks.setter
    def number_of_blocks(self, value):
        with timed_lock(self._number_of_blocks_lock.write_access, "_number_of_blocks_lock.write_access"):
            self._number_of_blocks = value

    @property
    def blocks(self):
        with timed_lock(self._blocks_lock.read_access, "_blocks_lock.read_access"):
            return self._blocks
    @blocks.setter
    def blocks(self, value):
        with timed_lock(self._blocks_lock.write_access, "_blocks_lock.write_access"):
            self._blocks = value
            
    @property
    def downloaded_blocks(self):
        with timed_lock(self._downloaded_blocks_lock.read_access, "_downloaded_blocks_lock.read_access"):
            return self._downloaded_blocks
    @downloaded_blocks.setter
    def downloaded_blocks(self, value):
        with timed_lock(self._downloaded_blocks_lock.write_access, "_downloaded_blocks_lock.write_access"):
            self._downloaded_blocks = value
            
    @property
    def requested_blocks(self):
        with timed_lock(self._requested_blocks_lock.read_access, "_requested_blocks_lock.read_access"):
            return self._requested_blocks
    @requested_blocks.setter
    def requested_blocks(self, value):
        with timed_lock(self._requested_blocks_lock.write_access, "_requested_blocks_lock.write_access"):
            self._requested_blocks = value
            
    @property
    def empty_blocks(self):
        with timed_lock(self._empty_blocks_lock.read_access, "_empty_blocks_lock.read_access"):
            return self._empty_blocks
    @empty_blocks.setter
    def empty_blocks(self, value):
        with timed_lock(self._empty_blocks_lock.write_access, "_empty_blocks_lock.write_access"):
            self._empty_blocks = value
            
    @property
    def cur_status(self):
        with timed_lock(self._cur_status_lock.read_access, "_cur_status_lock.read_access"):
            return self._cur_status
    @cur_status.setter
    def cur_status(self, value):
        with timed_lock(self._cur_status_lock.write_access, "_cur_status_lock.write_access"):
            self._cur_status = value

    def _init_blocks(self):
        if self.number_of_blocks > 1:
            for i in range(self.number_of_blocks):
                self._blocks.append(Block())
                self.empty_blocks.add(i)
            if self.piece_size % BLOCK_SIZE > 0:
                self._blocks[self.number_of_blocks - 1].block_size = self.piece_size % BLOCK_SIZE
        else:
            self._blocks.append(Block(self.piece_size))
            self.empty_blocks.add(1)  
        
    def changeStatus(self, block_index, status: Status, data=None, last_requested=None) -> tuple[Status, int] : 
        if 0 <= block_index < self.number_of_blocks:
            block: Block = self.blocks[block_index]
            real_status = block.changeStatus(status)
            if real_status == status:
                if data is not None:
                    block.data = data
                if last_requested is not None:
                    block.last_requested = last_requested
            d = len(self.downloaded_blocks)
            self._fix_sets(real_status, block_index)    
            return self.getStatus(), len(self.downloaded_blocks) - d
        
        return None
        
    def _fix_sets(self, real_status, block_index):
        if real_status == Status.EMPTY:
            self.empty_blocks.add(block_index)
            self.downloaded_blocks.discard(block_index)
            self.requested_blocks.discard(block_index)
        elif real_status == Status.REQUESTED:
            self.empty_blocks.discard(block_index)
            self.downloaded_blocks.discard(block_index)
            self.requested_blocks.add(block_index)
        elif real_status == Status.RECEIVED:
            self.empty_blocks.discard(block_index)
            self.downloaded_blocks.add(block_index)
            self.requested_blocks.discard(block_index)

        self._update_status()    
        
    def _update_status(self):
        if len(self.empty_blocks) == self.number_of_blocks:
            self.cur_status = Status.EMPTY
        if len(self.downloaded_blocks) == self.number_of_blocks:
            self.cur_status = Status.RECEIVED
        if self.requested_blocks:
            self.cur_status = Status.REQUESTED
        
    def getStatus(self):
        return self.cur_status
    
    def monitor_block_timeouts(self, timeout) -> Status:
        for block_index, block in enumerate(self.blocks):
            if block.status == Status.REQUESTED and block.last_requested:
                if time.time() - block.last_requested > timeout:
                    real_status = block.changeStatusForce(Status.EMPTY)
                    block.last_requested = None
                    self._fix_sets(real_status, block_index)    
            else:
                if block.status == Status.REQUESTED:
                    print(f"not last_requested {self.piece_index}:{block_index}")
                if block.status == Status.EMPTY:
                    print(f"empty {self.piece_index}:{block_index}")
                
                # else:
                    # print("not time.time() - block.last_requested > timeout:")     
            # else:
                # print("not block.status == Status.REQUESTED and block.last_requested")        

        return self.getStatus()
                    
    def is_block_empty(self, block_index):
        return self.blocks[block_index].status == Status.EMPTY
        