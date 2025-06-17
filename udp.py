import struct, random
from sys import int_info
import socket
class udpTrackerConnecting:
    def __init__(self):
        self.connection_id = struct.pack('>Q', 0x41727101980)
        self.action = struct.pack('>I', 0)
        trans_id = random.randint(0, 1000)
        self.transaction_id = struct.pack('>I', trans_id)
    def bytestringForConnecting(self):
        return self.connection_id + self.action + self.transaction_id
    def parse_response(self, data):
        self.action, = struct.unpack('>I', data[:4])
        self.trans_id, = struct.unpack('>I', data[4:8])
        self.server_connection_id, = struct.unpack('>Q', data[8:])

class udpTrackerAnnouncing:
    def __init__(self, conn_id, info_hash, peer_id, left, port):
        self.connection_id = struct.pack('>Q', conn_id)
        self.action = struct.pack('>I', 1)
        trans_id = random.randint(0, 1000)
        self.transaction_id = struct.pack('>I', trans_id)
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.downloaded = struct.pack('>Q', 0)
        self.left = struct.pack('>Q', left)
        self.uploaded = struct.pack('>Q', 0)
        self.event = struct.pack('>I', 0)
        self.ip = struct.pack('>I', 0)
        self.key = struct.pack('>I', 0)
        self.num_want = struct.pack('>I', 30)
        self.port = struct.pack('>H', 8000)
    def byteStringAnnounce(self):
        return (self.connection_id + self.action + self.transaction_id + self.info_hash + self.peer_id 
                + self.downloaded + self.left + self.uploaded + self.event + self.ip + self.key + self.num_want + self.port)
    
    def parse_response(self, response):
        if len(response) < 20:  # Minimum response size
            return []
            
        try:
            server_action = struct.unpack('>I', response[:4])[0]
            transaction_id = struct.unpack('>I', response[4:8])[0]
            interval = struct.unpack('>I', response[8:12])[0]
            leechers = struct.unpack('>I', response[12:16])[0]
            seeders = struct.unpack('>I', response[16:20])[0]
            
            ip_port = []
            offset = 20
            
            # Parse peer information
            while offset + 6 <= len(response):
                ip_bytes = response[offset:offset+4]
                port_bytes = response[offset+4:offset+6]
                
                ip = socket.inet_ntoa(ip_bytes)
                port = struct.unpack('>H', port_bytes)[0]
                
                ip_port.append((ip, port))
                offset += 6
                
            return ip_port
        except Exception as e:
            log_error(f"Error parsing UDP tracker response: {e}")
            return []
