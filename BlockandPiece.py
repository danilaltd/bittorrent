BLOCK_SIZE = 16384
from math import ceil
import random
import hashlib
from enum import Enum

class BlockStatus(Enum):
    EMPTY = 0
    REQUESTED = 1
    RECEIVED = 2

class Block:
    def __init__(self, block_size = BLOCK_SIZE, raw_bytes = b""):
        self.block_size = block_size
        self.data = raw_bytes
        self.status = BlockStatus.EMPTY
        self.last_requested = None

class Piece:
    def __init__(self, piece_index, piece_size, piece_sha1):
        self.piece_index = piece_index
        self.piece_size = piece_size
        self.piece_sha1 = piece_sha1
        self.number_of_blocks = ceil(self.piece_size / BLOCK_SIZE)
        self.blocks: list[Block] = []
        self.init_blocks()

    def init_blocks(self):
        if self.number_of_blocks > 1:
            for i in range(self.number_of_blocks):
                self.blocks.append(Block())
            if self.piece_size % BLOCK_SIZE > 0:
                self.blocks[self.number_of_blocks - 1].block_size = self.piece_size % BLOCK_SIZE
        else:
            self.blocks.append(Block(self.piece_size))
    
    def is_complete(self):
        for block in self.blocks:
            if block.status == BlockStatus.EMPTY or not block.data:
                return False
        return True  
    
    def is_requested(self):
        for block in self.blocks:
            if block.status == BlockStatus.REQUESTED:
                return True
        return False        

    def verify_piece(self):
        if not self.is_complete():
            return False
        piece_data = b"".join(block.data for block in self.blocks)
        return hashlib.sha1(piece_data).digest() == self.piece_sha1

    def get_empty_block(self):
        return random.choice(range(len(self.blocks)))
        
    # def allBlocksRequested(self):
    #     for block in self.blocks:
    #         if block.last_requested == False:
    #             return False
    #     return True