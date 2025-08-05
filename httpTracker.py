import struct
import socket
import time
from enum import Enum
from bcoding import bdecode
import requests
import os
from datetime import datetime

def log_error(msg, exc=None):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if exc is not None:
            log_entry = f'[{timestamp}][Error] {msg}: {exc}\n'
        else:
            log_entry = f'[{timestamp}][Error] {msg}\n'
            
        with open(os.path.join('logs', "bittorrent.log"), 'a', encoding='utf-8') as f:
            f.write(log_entry)

class HTTPEvent(Enum):
    started = 'started'
    completed = 'completed'
    stopped = 'stopped'

class httpTracker:
    def __init__(self, url, info_hash: bytes, peer_id: bytes, port: int):
        self.url = url
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.port = port
        self.announce_interval: int = 1
        self._notified_start = False
        self.attempts = 0
        self.announce_fault = False
        self.last_transmition = time.time()
        self.need_to_notify = False
        self.last_left = None
        
    def announce(self, downloaded: int, left: int, uploaded: int, port: int, event: HTTPEvent | None = None) -> list[tuple[str, int]]:
        self.last_transmition = time.time()
        if self.announce_fault:
            self.attempts += 1
        self.announce_fault = True
        self.announce_interval = 15
        payload = {
            'info_hash': self.info_hash, 
            'peer_id': self.peer_id, 
            'port': str(self.port), 
            'uploaded': str(uploaded), 
            'downloaded': str(downloaded), 
            'left': str(left), 
            'numwant': str(200), 
        }
        
        if event is None:
            if not self._notified_start:
                event = HTTPEvent.started
                self._notified_start = True
            elif ((self.last_left is None or self.last_left != 0) and left == 0):
                event = HTTPEvent.completed
                

        if event is not None:
            payload['event'] = event.value
        self.last_left = left

        try:
            try:
                answer_tracker = requests.get(self.url, params=payload, timeout=5)
            except Exception as e:
                raise Exception(f"requests error. url: {self.url}, err: {e}")
            try:
                response = bdecode(answer_tracker.content)
            except Exception as e:
                raise Exception(f"bdecode error: {e}; contrnt: {answer_tracker.content}; status_code: {answer_tracker.status_code}")
            res: list[tuple[str, int]] = set()
            if 'failure reason' in response:
                log_error(f"continue before exception but {self.url} failure reason: {response['failure reason']}")
            if 'interval' in response and 'peers' in response:
                announce_interval = response['interval']
                peers = response['peers']
            else:
                raise Exception(f" 'interval' in response and 'peers' in response == False. response: {response}")
            
            if type(peers) != dict:
                if len(peers) % 6 != 0:
                    raise Exception(f"len(peers) % 6 != 0. len: {len(peers)}; type:{type(peers)}; content: {peers}")
                    
                offset=0
                for _ in range(len(peers)//6):
                    ip = socket.inet_ntoa(peers[offset:offset + 4])
                    offset += 4
                    port = struct.unpack("!H", peers[offset : offset + 2])[0]
                    offset += 2
                    ip_port = (ip,port)
                    res.add(ip_port)
            else:
                for p in peers:
                    ip_port = (p['ip'], p['port'])
                    res.add(ip_port)
                    
            self.attempts = 0
            self.announce_fault = False
            self.announce_interval = announce_interval
            self.need_to_notify = False
            return res
        except Exception as e:
            raise Exception(f"http_request: {e}")