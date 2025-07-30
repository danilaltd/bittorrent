import struct
# from bitstring import *
import bitstring
class keep_alive:
    # keep-alive: <len=0000>
    length = 0
    def __init__(self):
        return
    @classmethod
    def byteStringForKeepAlive(cls):
        return struct.pack('>I', cls.length)
    
class chocke:
    # choke: <len=0001><id=0>
    length = 1
    message_ID = 0

    def __init__(self) -> None:
        pass
    @classmethod
    def to_bytes(self):
        return struct.pack(">IB", self.length, self.message_ID)
    @classmethod
    def parse_response(self, response):
        length, message_ID = struct.unpack(">IB", response[:5])
        if length == self.length and message_ID == self.message_ID:
            return 1
        else: return -1

class unchoke:
    # unchoke: <len=0001><id=1>
    length = 1
    message_ID = 1

    def __init__(self):
        return
    @classmethod
    def parse_response(self, response):
        length, message_id = struct.unpack('>IB', response[:5])
        if length == self.length and message_id == self.message_ID:
            return 1
        else:
            return -1
            
    def byteStringForUnchoke(self):
        return struct.pack('>IB', self.length, self.message_ID)

class interested:
    # interested: <len=0001><id=2>
    length = 1
    message_ID = 2
    def __init__(self):
        return
    def byteStringForInterested(self):
        return struct.pack('>IB', self.length, self.message_ID)

class not_interested:
    # not interested: <len=0001><id=3>
    length = 1
    message_ID = 3
    def __init__(self) -> None:
        pass
    def byteStringForNotInterested(self):
        return struct.pack(">IB", self.length, self.message_ID)
    
class have:
    # have: <len=0005><id=4><piece index>
    length = 5
    message_ID = 4
    def __init__(self, piece_index) -> None:
        self.piece_index = piece_index
    @classmethod
    def parse_response(self, response):
        length, message_ID, piece_index = struct.unpack(">IBI", response[:9])
        if length == self.length and message_ID == self.message_ID:
            return have(piece_index)
        return -1

class Bitfield:
    # bitfield: <len=0001+X><id=5><bitfield>
    message_ID = 5
    def __init__(self, bitfield: bitstring.BitArray):
        self.length = len(bitfield) // 8 + 1
        self.bitfield = bitfield
        
    def byteStringForBitfield(self):
        return struct.pack(">IB", self.length, self.message_ID) + self.bitfield.tobytes()

class request:
    # request: <len=0013><id=6><index><begin><length>
    length = 13
    message_ID = 6
    def __init__(self, piece_index, piece_offset, block_length):
        self.piece_index = piece_index
        self.piece_offset = piece_offset
        self.block_length = block_length
    def byteStringForRequest(self):
        return struct.pack(">IBIII", self.length, self.message_ID, self.piece_index, self.piece_offset, self.block_length)

class pieceMessage:
    # piece: <len=0009+X><id=7><index><begin><block>
    length = None
    message_ID = 7
    def __init__(self):
        return
    @classmethod
    def parse_response(cls, response):
        block_length = len(response) - 13
        length, message_ID, piece_index, block_offset , block= struct.unpack(f">IBII{block_length}s", response)
        if cls.message_ID == message_ID:
            return (piece_index, block_length, block_offset, block)      
        else:
            return -1
        
class cancel:
    # cancel: <len=0013><id=8><index><begin><length>
    length = 13
    message_ID = 8
    def __init__(self, piece_index, piece_offset, block_length):
        self.piece_index = piece_index
        self.piece_offset = piece_offset
        self.block_length = block_length
    def byteStringForCancel(self):
        return struct.pack(">IBIII", self.length, self.message_ID, self.piece_index, self.piece_offset, self.block_length)

class Handshake():
    def __init__(self, peer_id, info_hash):
        pstr = b"BitTorrent protocol"
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.pstr = pstr
        self.pstrlen = len(pstr)
        self.reserved = b'x\00' * 8
        self.handshake = struct.pack(">B{}s8s20s20s".format(self.pstrlen),
                            self.pstrlen,
                            self.pstr,
                            self.reserved,
                            self.info_hash,
                            self.peer_id)
    def getHandshakeBytes(self):
        return self.handshake






