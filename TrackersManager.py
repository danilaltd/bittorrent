from torrent import Torrent
from bcoding import bdecode
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
        self.sock.setblocking(False)
                
        self._connect_to_trackers()

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
                log_error(f"Error in _periodic_update loop: {e}")

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
        self.trackers_update_thread.daemon = True
        self.trackers_update_thread.start()

    def stop_periodic_updates(self):
        self.running = False
        for tracker in self.udp_trackers:
            self.sock.sendto(tracker.bytes_for_announce(self.downloaded, self.left, self.uploaded, self.port, UDPEvent.stopped), tracker.sock_addr)
        for tracker in self.http_trackers:
            self._announcee_http(tracker, HTTPEvent.stopped)
            
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