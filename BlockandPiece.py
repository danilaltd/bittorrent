BLOCK_SIZE = 16384
from math import ceil
import hashlib
from enum import Enum
from rwlock import RWLock
from rwlock import timed_lock

class BlockStatus(Enum):
    EMPTY = 0
    REQUESTED = 1
    RECEIVED = 2

class Block:
    def __init__(self, block_size = BLOCK_SIZE, raw_bytes = b""):
        self._block_size = block_size
        self._block_size_lock = RWLock("_block_size_lock")
        self._data = raw_bytes
        self._data_lock = RWLock("_data_lock")
        self._status = BlockStatus.EMPTY
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

    def _init_blocks(self):
        if self.number_of_blocks > 1:
            for i in range(self.number_of_blocks):
                self._blocks.append(Block())
            if self.piece_size % BLOCK_SIZE > 0:
                self._blocks[self.number_of_blocks - 1].block_size = self.piece_size % BLOCK_SIZE
        else:
            self._blocks.append(Block(self.piece_size))
    
    def is_complete(self):
        with timed_lock(self._blocks_lock.read_access, "_blocks_lock.read_access"):
            for block in self._blocks:
                if block.status == BlockStatus.EMPTY or not block.data:
                    return False
            return True  
    
    def is_requested(self):
        with timed_lock(self._blocks_lock.read_access, "_blocks_lock.read_access"):
            for block in self._blocks:
                if block.status == BlockStatus.REQUESTED:
                    return True
            return False        