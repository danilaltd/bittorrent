from torrent import Torrent
import ipaddress
from bcoding import bencode, bdecode
from udp import udpTrackerAnnouncing, udpTrackerConnecting
import requests
import struct
import random
import socket
import time
import errno

from urllib.parse import urlparse
import threading
import sys

def log_info(msg):
    GREEN = '\033[92m'
    END = '\033[0m'
    print(f'{GREEN}[INFO] {msg}{END}')

def log_error(msg, exc=None):
    RED = '\033[91m'
    END = '\033[0m'
    if exc is not None:
        print(f'{RED}[ERROR] {msg}: {exc}{END}', file=sys.stderr)
    else:
        print(f'{RED}[ERROR] {msg}{END}', file=sys.stderr)

class Tracker:
    def __init__(self, torrent_path, file_path):
        self.torrent_obj = Torrent(torrent_path, file_path) 
        self.peers = set()
        self.tracker_threads = []
        self.tracker_update_thread = None
        self.downloaded = 0
        self.uploaded = 0
        self.left = self.torrent_obj.total_length
        self.last_update = time.time()
        self.update_interval = 1800  # Default interval in seconds
        self.running = True

    def start_periodic_updates(self):
        """Start periodic tracker updates in a separate thread"""
        self.tracker_update_thread = threading.Thread(target=self._periodic_update)
        self.tracker_update_thread.daemon = True
        self.tracker_update_thread.start()

    def stop_periodic_updates(self):
        """Stop periodic tracker updates"""
        self.running = False
        if self.tracker_update_thread:
            self.tracker_update_thread.join()

    def _periodic_update(self):
        """Periodically update trackers with current stats"""
        while self.running:
            try:
                # Wait for the update interval
                time.sleep(self.update_interval)
                
                # Update all trackers
                for url in self.torrent_obj.announce_list:
                    t = None
                    if "http" in url:
                        t = threading.Thread(target=self.http_request, args=(url, 'update'))
                    elif "udp" in url:
                        t = threading.Thread(target=self.udp_request, args=(url, 'update'))
                    if t:
                        t.start()
                        self.tracker_threads.append(t)
                
                # Clean up completed threads
                self.tracker_threads = [t for t in self.tracker_threads if t.is_alive()]
                
            except Exception as e:
                log_error(f"Error in periodic tracker update: {e}")

    def update_stats(self, downloaded, uploaded):
        """Update download/upload statistics"""
        self.downloaded = downloaded
        self.uploaded = uploaded
        self.left = max(0, self.torrent_obj.total_length - downloaded)

    def get_peer_list(self):
        """Initial peer list request"""
        for url in self.torrent_obj.announce_list:
            t = None
            if "http" in url:
                t = threading.Thread(target=self.http_request, args=(url, 'started'))
            if "udp" in url:
                t = threading.Thread(target=self.udp_request, args=(url, 'started'))
            if t:
                t.start()
                self.tracker_threads.append(t)

    def exitAllThreads(self):
        """Stop all tracker communication threads"""
        self.stop_periodic_updates()
        for thread in self.tracker_threads:
            thread.join()

    def http_request(self, tracker_url, event='started'):
        url_parse = urlparse(tracker_url)
        payload = {
            'info_hash': self.torrent_obj.info_hash, 
            'peer_id': self.torrent_obj.peer_id, 
            'uploaded': self.uploaded, 
            'downloaded': self.downloaded, 
            'port': 6889, 
            'left': self.left, 
            'event': event
        }
        
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                answer_tracker = requests.get(tracker_url, params=payload, timeout=5)
                response = bdecode(answer_tracker.content)
                
                if 'interval' in response:
                    self.update_interval = response['interval']
                
                if 'peers' not in response:
                    log_error(f"No peers in response from {tracker_url}")
                    return
                    
                if isinstance(response['peers'], bytes):
                    # Binary format
                    offset = 0
                    while offset < len(response['peers']):
                        if offset + 6 > len(response['peers']):
                            break
                        ip = struct.unpack_from("!i", response['peers'], offset)[0]
                        ip = socket.inet_ntoa(struct.pack("!i", ip))
                        offset += 4
                        port = struct.unpack_from("!H", response['peers'], offset)[0]
                        offset += 2
                        ip_port = (ip, port)
                        self.peers.add(ip_port)
                elif isinstance(response['peers'], list):
                    # Dictionary format
                    for peer in response['peers']:
                        if isinstance(peer, dict) and 'ip' in peer and 'port' in peer:
                            ip_port = (peer['ip'], peer['port'])
                            self.peers.add(ip_port)
                else:
                    log_error(f"Unknown peer format from {tracker_url}")
                    return
                    
                log_info(f"Successfully got {len(self.peers)} peers from {tracker_url}")
                return  # Success, exit the function
            except requests.exceptions.ConnectionError as e:
                if "Name or service not known" in str(e) or "DNS resolution failed" in str(e):
                    log_error(f"DNS resolution failed for {tracker_url} (attempt {attempt + 1}/{max_retries})", e)
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                        continue
                log_error(f"Connection error for {tracker_url}", e)
                return
            except Exception as e:
                log_error(f"Error connecting to tracker {tracker_url}", e)
                return

    def udp_request(self, tracker_url, event='started'):
        url_parse = urlparse(tracker_url)
        tracker_connection = udpTrackerConnecting()
        
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)  
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.sendto(tracker_connection.bytestringForConnecting(), (url_parse.hostname, url_parse.port))
                break  # Success, exit the retry loop
            except socket.gaierror as e:
                log_error(f'DNS resolution failed (IPv4) (attempt {attempt + 1}/{max_retries})', e)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                # Try IPv6 as fallback
                try:   
                    sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
                    sock.sendto(tracker_connection.bytestringForConnecting(), (url_parse.hostname, url_parse.port, 0, 0))
                except Exception as e:
                    log_error('Failed to connect via IPv6', e)
                    return
            except Exception as e:
                log_error('Error sending UDP packet', e)
                return

        sock.settimeout(10) 
        try:
            data, addr = sock.recvfrom(131072)
        except socket.timeout:
            log_error('Таймаут ожидания ответа от UDP-трекера')
            return
        except Exception as e:
            log_error('Ошибка при получении ответа от UDP-трекера', e)
            return

        tracker_connection.parse_response(data)
        sender_port = sock.getsockname()[1]

        tracker_announce = udpTrackerAnnouncing(
            tracker_connection.server_connection_id,
            self.torrent_obj.info_hash,
            self.torrent_obj.peer_id,
            self.left,
            sender_port
        )

        try:
            sock.sendto(tracker_announce.byteStringAnnounce(), (url_parse.hostname, url_parse.port))
        except socket.gaierror as e:
            log_error('Ошибка отправки announce (IPv4)', e)
            sock = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            try:    
                sock.sendto(tracker_announce.byteStringAnnounce(), (url_parse.hostname, url_parse.port, 0, 0))
            except Exception as e:
                log_error('Ошибка отправки announce (IPv6)', e)
                return

        sock.settimeout(10) 
        completeMessage = b'' 

        while True:
            try:
                data, addr = sock.recvfrom(4096)
                if len(data) <= 0:
                    break
                completeMessage += data
            except socket.error as e:
                err = e.args[0]
                if err != errno.EAGAIN or err != errno.EWOULDBLOCK:
                    log_error('Ошибка socket при получении данных от трекера', e)
                break
            except Exception as e:
                log_error('Неизвестная ошибка при получении данных от трекера', e)
                break

        if len(completeMessage) <= 0:
            log_error('Не удалось получить список пиров от трекера')
            return

        ip_ports = tracker_announce.parse_response(completeMessage)
        for x in ip_ports:
            self.peers.add(x)
        log_info(f'Получено {len(ip_ports)} пиров от трекера')