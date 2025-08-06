import struct, random
import socket
import time
from enum import Enum

class UDPEvent(Enum):
    none = 0
    completed = 1
    started = 2
    stopped = 3

class Action(Enum):
    connect = 0
    announce = 1
    scrape = 2
    error = 3
            
class udpTracker:
    def __init__(self, url, info_hash: bytes, peer_id: bytes):
        self.url = url
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.sock_addr = None
        self.last_send: Action = None
        self.last_transaction_id: int = None
        self.server_connection_id: int = None
        self.announce_interval: int = 99999
        self.last_transmition = time.time()
        self.last_announce = time.time()
        self.attempts = 0
        self.last_request = bytes()
        self.pending = False
        self._notified_start = False
        self.initialised = False
        self._need_to_notify = False
        self.last_left = None
    
    def bytes_for_connecting(self):
        connection_id = struct.pack('>q', 0x41727101980)
        action = struct.pack('>i', Action.connect.value)
        self.last_transaction_id = random.randint(-2147483648, 2147483647)
        transaction_id = struct.pack('>i', self.last_transaction_id)
        res = connection_id + action + transaction_id
        self.last_request = res
        self.attempts = 0
        self.pending = True
        self.last_transmition = time.time()
        return res
    
    def bytes_for_announce(self, downloaded: int, left: int, uploaded: int, port: int, event: UDPEvent | None = None):
        connection_id = struct.pack('>q', self.server_connection_id)
        action = struct.pack('>i', Action.announce.value)
        self.last_transaction_id = random.randint(-2147483648, 2147483647)
        transaction_id = struct.pack('>i', self.last_transaction_id)
        downloaded_bytes = struct.pack('>q', downloaded) #downloaded bytes
        left_bytes = struct.pack('>q', left) #left bytes
        uploaded_bytes = struct.pack('>q', uploaded) #uploaded bytes
        if event is None:
            if not self._notified_start:
                self._notified_start = True
                event = UDPEvent.started
            elif ((self.last_left is None or self.last_left != 0) and left == 0):
                event = UDPEvent.completed
            else:
                event = UDPEvent.none
        event_bytes = struct.pack('>i', event.value)
        ip = struct.pack('>I', 0)
        key = struct.pack('>I', random.randint(0, 4294967295))
        num_want = struct.pack('>i', -1)
        port_bytes = struct.pack('>H', port)
        res = connection_id + action + transaction_id + self.info_hash + self.peer_id + downloaded_bytes + left_bytes + uploaded_bytes + event_bytes + ip + key + num_want + port_bytes
        self.last_request = res
        self.attempts = 0
        self.pending = True
        self.last_transmition = time.time()
        self.last_announce = time.time()
        self._need_to_notify = False
        return res
    
    def bytes_for_scraping(self, info_hashes: list[bytes]):
        connection_id = struct.pack('>q', self.server_connection_id)
        action = struct.pack('>i', Action.scrape.value)
        self.last_transaction_id = random.randint(-2147483648, 2147483647)
        transaction_id = struct.pack('>i', self.last_transaction_id)
        info_hashes_bytes = b''.join(info_hashes)
        res = connection_id + action + transaction_id + info_hashes_bytes
        self.last_request = res
        self.attempts = 0
        self.pending = True
        self.last_transmition = time.time()
        return res
    
    def resend(self):
        self.last_transmition = time.time()
        self.attempts += 1
        return self.last_request
    
    def _parse_connection_response(self, data: bytearray):
        if len(data) != 16:
            raise Exception(f"parse_connection_response: wrong data len. len(data):{len(data)}, not 16")
        self.server_connection_id, = struct.unpack(">q", data[8:16])
        self.announce_interval: int = 1
        self.initialised = True
    
    def _parse_announce_response(self, data: bytearray) -> list[tuple[str, int]]:
        if len(data) < 20 or (len(data) - 20) % 6 != 0:
            raise Exception(f"parse_announce_response: wrong data len. len(data):{len(data)}, len(data) < 20 or (len(data) - 20) % 6 != 0")
        self.announce_interval, leechers, seeders = struct.unpack(">iii", data[8:20])
        res: list[tuple[str, int]] = []
        offset = 20
        while offset != len(data):
            ip_str = socket.inet_ntoa(data[offset:offset+4])
            port, = struct.unpack('>H', data[offset+4:offset+6])
            offset += 6
            res.append((ip_str, port))
        
        return res
        
    def _parce_scraping_response(self, data: bytearray) -> list[tuple[int, int, int]]:
        if (len(data) - 8) % 12 != 0:
            raise Exception(f"parce_scraping_response: wrong data len. len(data):{len(data)}, (len(data) - 8) % 12 != 0")
        res: list[tuple[int, int, int]] = []
        offset = 8
        while offset != len(data):
            stats = struct.unpack('>iii', data[offset:offset+12])
            offset += 12
            res.append(stats)
        return res

    def _parce_error_response(self, data: bytearray):
        raise Exception(data[8:].decode('ascii'))
    
    def parce_response(self, data: bytearray) -> list[tuple[str, int]]:
        res = []
        action, trans_id = struct.unpack(">ii", data[:8])
        if trans_id != self.last_transaction_id:
            raise Exception (f"trans_id != self.transaction_id: {trans_id} {self.last_transaction_id}")
        if (action == Action.connect.value):
            self._parse_connection_response(data)
        elif (action == Action.announce.value):
            res = self._parse_announce_response(data)
        elif (action == Action.scrape.value):
            self._parce_scraping_response(data)
        elif (action == Action.error.value):
            self._parce_error_response(data)
        else:
            raise Exception (f"not supported action: {action}")
        self.pending = False
        return res
        
