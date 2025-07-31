from torrent import Torrent
from udpTracker import udpTracker, UDPEvent
from httpTracker import httpTracker, HTTPEvent
import struct
import socket
import time
from urllib.parse import urlparse
import threading
from datetime import datetime
import os
import traceback

SOCKET_READ_INTERVAL = 60
TRACKER_TIMEOUT_CHECK_INTERVAL = 7
TRACKER_RECIEVE_TIMEOUT = 15
TRACKER_MAX_ATTEMPTS_TO_RECIEVE = 4

def log_info(msg):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f'[{timestamp}][INFO] {msg}\n'
            
        with open(os.path.join('logs', "bittorrent.log"), 'a', encoding='utf-8') as f:
            f.write(log_entry)

def log_error(msg, exc=None):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if exc is not None:
            log_entry = f'[{timestamp}][Error] {msg}: {exc}\n'
        else:
            log_entry = f'[{timestamp}][Error] {msg}\n'
            
        with open(os.path.join('logs', "bittorrent.log"), 'a', encoding='utf-8') as f:
            f.write(log_entry)
    

class TrackersManager:
    def __init__(self, torrent_path, file_path):
        self.torrent_obj = Torrent(torrent_path, file_path) 
        self.peers: set[tuple [str, int]] = set()
        self.peers_lock = threading.Lock()

        self.trackers_update_thread = None
        self.downloaded = 0
        self.uploaded = 0
        self.left = self.torrent_obj.total_length
        self.last_update = time.time()
        self.running = True
        self.socket_last_read_time = time.time()
        self.trackers_last_update_time = time.time()
        self.udp_trackers: list[udpTracker] = []
        self.http_trackers: list[httpTracker] = []
        self.port = self.torrent_obj.port
        self.udp_initialised = False
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('0.0.0.0', 0))
        self.sock.setblocking(False)
        
        self._connect_to_trackers()
        
        #music
        # self.peers.add(('5.79.98.162', 53916))
        # self.peers.add(('5.79.98.162', 50253))
        # self.peers.add(('90.240.225.228', 31550))
        # self.peers.add(('93.103.120.131', 62975))
        # self.peers.add(('78.61.182.22', 19591))
        
        #680
        # self.peers.add(('5.16.191.73', 6881))
        # self.peers.add(('95.72.142.61', 14490))
        # self.peers.add(('95.84.31.226', 17474))
        # self.peers.add(('79.164.147.5', 44283))
        # self.peers.add(('5.3.116.8', 41912))
        # self.peers.add(('178.47.99.255', 31789))
        # self.peers.add(('188.243.42.150', 32076))
        # self.peers.add(('5.3.121.203', 51413))
        # self.peers.add(('88.201.232.191', 50207))
        # self.peers.add(('88.206.117.228', 6881))
        # self.peers.add(('95.83.48.149', 6881))
        # self.peers.add(('95.220.20.97', 59692))
        # self.peers.add(('46.147.103.212', 51461))
        # self.peers.add(('77.35.93.159', 6881))
        # self.peers.add(('85.113.60.103', 41044))
        # self.peers.add(('92.101.170.91', 6882))
        # self.peers.add(('108.21.185.107', 6881))
        
        #300
        # self.peers.add(('178.217.106.31',  27469))
        # self.peers.add(('212.104.70.181',  6881))
        # self.peers.add(('94.51.78.253',    64485))
        # self.peers.add(('193.142.114.236', 13708))
        # self.peers.add(('80.242.101.249',  23762))
        # self.peers.add(('244.91.41.46',    19876))
        # self.peers.add(('188.243.68.30',   10747))
        # self.peers.add(('94.180.238.63',   62539))
        # self.peers.add(('194.135.151.209', 35201))
        # self.peers.add(('77.236.67.140',   6881))
        
        #photoshop
        # self.peers.add(('188.243.149.133',   36525))
        # self.peers.add(('77.45.139.120',   15705))
        # self.peers.add(('178.22.51.56',   56483))
        # self.peers.add(('93.100.77.65',   15255))
        # self.peers.add(('46.152.0.51',   19852))
        # self.peers.add(('31.10.81.195',   18551))
        # self.peers.add(('188.237.186.23',   34213))
        # self.peers.add(('2.50.253.194',   19858))
        # self.peers.add(('60.240.184.180',   6881))
        # self.peers.add(('24.246.62.146',   63047))
        # self.peers.add(('89.143.163.187',   3501))
        # self.peers.add(('212.60.76.115',   6881))
        # self.peers.add(('79.120.76.92',   51413))
        # self.peers.add(('185.68.145.173',   6881))
        # self.peers.add(('94.181.140.250',   41285))
        # self.peers.add(('188.128.36.163',   10875))
        # self.peers.add(('5.166.90.230',   10111))
        # self.peers.add(('95.55.42.138',   6881))
        # self.peers.add(('178.140.175.180',   32644))
        # self.peers.add(('95.29.231.233',   52248))
        # self.peers.add(('46.0.6.43',   6881))
        # self.peers.add(('5.167.93.48',   48523))
        # self.peers.add(('178.214.243.240',   16881))
        # self.peers.add(('92.101.161.7',   24090))
        # self.peers.add(('94.190.23.99',   50611))
        # self.peers.add(('37.112.197.202',   19307))
        
        # self.peers.add(('176.195.240.222',   47950))
        
        
        self.peers.add(('83.37.145.154',   45680)) #andr
        self.peers.add(('109.110.74.209',   14648)) #andr
        # self.peers.add(('221.162.198.176',   44555)) #ninja


    def _connect_to_trackers(self):
        for url in self.torrent_obj.announce_list:
            try:
                if "http://" in url:
                    self._connect_http(url)
                elif "udp://" in url:
                    self._connect_udp(url)
            except Exception as e:
                log_error(f"Error in connect_to_trackers: {url} {e} \nTraceback:\n{traceback.format_exc()}")
    
    def _connect_http(self, tracker_url: str):
        tracker = httpTracker(tracker_url, self.torrent_obj.info_hash, self.torrent_obj.peer_id, self.port)
        self.http_trackers.append(tracker)
        
    def _connect_udp(self, tracker_url: str):
        retry_delay=0.5
        url_parse = urlparse(tracker_url)
        tracker = udpTracker(tracker_url, self.torrent_obj.info_hash, self.torrent_obj.peer_id)
        try:
            addrinfo_list = socket.getaddrinfo(url_parse.hostname, url_parse.port, socket.AF_UNSPEC, socket.SOCK_DGRAM)
        except socket.gaierror as e:
            log_error(f"DNS resolution failed: url_parse.hostname: {url_parse.hostname}, url_parse.port: {url_parse.port}; {e}")
            return
        connecting_bytes = tracker.bytes_for_connecting()
        for family, socktype, proto, canonname, sockaddr in addrinfo_list:
            try:
                self.sock.sendto(connecting_bytes, sockaddr)
            except Exception as e:
                time.sleep(retry_delay)
                continue
            self.udp_trackers.append(tracker)
            tracker.sock_addr = sockaddr
            return
        log_error(f"{tracker_url}: UDP failed")

    def _periodic_update(self):
        """Periodically update trackers with current stats"""
        while self.running:
            try:
                current_time = time.time()
                if (not self.udp_initialised and current_time - self.socket_last_read_time >= 1) or current_time - self.socket_last_read_time >= SOCKET_READ_INTERVAL:
                    self._parse_response_udp()
                    self.socket_last_read_time = current_time
                
                if current_time - self.trackers_last_update_time >= TRACKER_TIMEOUT_CHECK_INTERVAL:
                    udp_trackers_to_remove = []
                    udp_initialised = True
                    for tracker in self.udp_trackers:
                        udp_initialised |= tracker.initialised
                        if tracker.pending and current_time - tracker.last_transmition >= TRACKER_RECIEVE_TIMEOUT:
                            if tracker.attempts >= TRACKER_MAX_ATTEMPTS_TO_RECIEVE:
                                udp_trackers_to_remove.append(tracker)
                            else:
                                self.sock.sendto(tracker.resend(), tracker.sock_addr)
                        elif current_time - tracker.last_announce >= tracker.announce_interval:
                            self.sock.sendto(tracker.bytes_for_announce(self.downloaded, self.left, self.uploaded, self.port), tracker.sock_addr)
                    self.udp_initialised = udp_initialised
                            
                    for tracker in udp_trackers_to_remove:
                        self.udp_trackers.remove(tracker)
                    
                    http_trackers_to_remove = []
                    for tracker in self.http_trackers:
                        if current_time - tracker.last_transmition >= TRACKER_RECIEVE_TIMEOUT:
                            if tracker.attempts >= TRACKER_MAX_ATTEMPTS_TO_RECIEVE:
                                http_trackers_to_remove.append(tracker)
                            else:
                                self._announcee_http(tracker)
                        elif current_time - tracker.last_transmition >= tracker.announce_interval:
                            self._announcee_http(tracker)

                    for tracker in http_trackers_to_remove:
                        self.http_trackers.remove(tracker)
                    
                    self.trackers_last_update_time = current_time
                
                time.sleep(1)
            except Exception as e:
                log_error(f"Error in _periodic_update loop: {e} \nTraceback:\n{traceback.format_exc()}")
                

    def _get_tracker_by_transaction_id(self, transaction_id: int) -> udpTracker:
        for tracker in self.udp_trackers:
            if tracker.last_transaction_id == transaction_id:
                return tracker
        raise Exception(f"no tracker with last_transaction_id: {transaction_id}")
    
    def _parse_response_udp(self):
        while True:
            try:
                data, addr = self.sock.recvfrom(4096)
            except BlockingIOError:
                break 
            if len(data) < 8:
                raise Exception(f"parse_response: len(data) < 8, len(data): {len(data)}")
            transaction_id, = struct.unpack(">i", data[4:8])
            try:
                peers = self._get_tracker_by_transaction_id(transaction_id).parce_response(data)
                with self.peers_lock:
                    self.peers.update(peers)
            except Exception as e:
                log_error(f"_parse_response_udp: {e}")
                
    def _announcee_http(self, tracker: httpTracker, event: HTTPEvent | None = None):
        try:
            peers = tracker.announce(self.downloaded, self.left, self.uploaded, self.port, event)
            with self.peers_lock:
                self.peers.update(peers)
        except Exception as e:
            log_error(f"_announce_http: {e}")
                
    def start_periodic_updates(self):
        self.trackers_update_thread = threading.Thread(target=self._periodic_update)
        # self.trackers_update_thread.daemon = True
        self.trackers_update_thread.start()

    def stop_periodic_updates(self):
        self.running = False
        for tracker in self.udp_trackers:
            try:
                self.sock.sendto(tracker.bytes_for_announce(self.downloaded, self.left, self.uploaded, self.port, UDPEvent.stopped), tracker.sock_addr)
            except:
                pass
        for tracker in self.http_trackers:
            try:
                self._announcee_http(tracker, HTTPEvent.stopped)
            except:
                pass
            
    def notify_trackers_complete(self):
        for tracker in self.udp_trackers:
            self.sock.sendto(tracker.bytes_for_announce(self.downloaded, self.left, self.uploaded, self.port, UDPEvent.completed), tracker.sock_addr)
        for tracker in self.http_trackers:
            self._announcee_http(tracker, HTTPEvent.completed)

    def update_stats(self, downloaded, uploaded):
        """Update download/upload statistics"""
        self.downloaded = downloaded
        self.uploaded = uploaded
        self.left = max(0, self.torrent_obj.total_length - downloaded)