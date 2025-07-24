from torrent import Torrent
from bcoding import bdecode
from udp import udpTrackerAnnouncing, udpTrackerConnecting
import requests
import struct
import socket
import time
import errno
from urllib.parse import urlparse
import threading
from datetime import datetime
import os
from logger import timed_lock

def log_info(msg):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f'[{timestamp}][Error][INFO] {msg}\n'
            
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
    

class Tracker:
    def __init__(self, torrent_path, file_path):
        self.torrent_obj = Torrent(torrent_path, file_path) 
        self.peers: set[tuple [str, int]] = set()
        #music
        # self.peers.add(('5.79.98.162', 53916))
        # self.peers.add(('90.240.225.228', 31550))
        
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
        self.peers.add(('188.243.149.133',   36525))
        self.peers.add(('77.45.139.120',   15705))
        self.peers.add(('178.22.51.56',   56483))
        self.peers.add(('93.100.77.65',   15255))
        self.peers.add(('46.152.0.51',   19852))
        self.peers.add(('31.10.81.195',   18551))
        self.peers.add(('188.237.186.23',   34213))
        self.peers.add(('2.50.253.194',   19858))
        self.peers.add(('60.240.184.180',   6881))
        self.peers.add(('24.246.62.146',   63047))
        self.peers.add(('89.143.163.187',   3501))
        self.peers.add(('212.60.76.115',   6881))
        
        self.peers.add(('79.120.76.92',   51413))
        self.peers.add(('185.68.145.173',   6881))
        self.peers.add(('94.181.140.250',   41285))
        self.peers.add(('188.128.36.163',   10875))
        self.peers.add(('5.166.90.230',   10111))
        self.peers.add(('95.55.42.138',   6881))
        self.peers.add(('178.140.175.180',   32644))
        self.peers.add(('95.29.231.233',   52248))
        self.peers.add(('46.0.6.43',   6881))
        self.peers.add(('5.167.93.48',   48523))
        self.peers.add(('178.214.243.240',   16881))
        self.peers.add(('92.101.161.7',   24090))
        self.peers.add(('94.190.23.99',   50611))
        self.peers.add(('37.112.197.202',   19307))
        
        

        self.tracker_threads = []
        self.tracker_update_thread = None
        self.downloaded = 0
        self.uploaded = 0
        self.left = self.torrent_obj.total_length
        self.last_update = time.time()
        self.update_interval = 1800  # Default interval in seconds
        self.running = True
        self.peers_lock = threading.Lock()

    def start_periodic_updates(self):
        """Start periodic tracker updates in a separate thread"""
        print("starting periodic updates")
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
            finally:
                # Wait for the update interval
                time.sleep(self.update_interval)    

    def update_stats(self, downloaded, uploaded):
        """Update download/upload statistics"""
        self.downloaded = downloaded
        self.uploaded = uploaded
        self.left = max(0, self.torrent_obj.total_length - downloaded)

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
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                # Try to resolve DNS first
                try:
                    socket.gethostbyname(url_parse.hostname)
                except socket.gaierror as e:
                    log_error(f"DNS resolution failed for {url_parse.hostname} (attempt {attempt + 1}/{max_retries})", e)
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (attempt + 1))
                        continue
                    return

                answer_tracker = requests.get(tracker_url, params=payload, timeout=10)  # Increased timeout
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
                        with self.peers_lock:
                            self.peers.add(ip_port)
                elif isinstance(response['peers'], list):
                    # Dictionary format
                    for peer in response['peers']:
                        if isinstance(peer, dict) and 'ip' in peer and 'port' in peer:
                            ip_port = (peer['ip'], peer['port'])
                            with self.peers_lock:
                                self.peers.add(ip_port)
                else:
                    log_error(f"Unknown peer format from {tracker_url}")
                    return
                    
                with self.peers_lock:
                    log_info(f"Successfully got {len(self.peers)} peers from {tracker_url}")
                return  # Success, exit the function
            except requests.exceptions.ConnectionError as e:
                log_error(f"Connection error for {tracker_url} (attempt {attempt + 1}/{max_retries})", e)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                return
            except Exception as e:
                log_error(f"Error connecting to tracker {tracker_url} (attempt {attempt + 1}/{max_retries})", e)
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
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
        with self.peers_lock:
            for x in ip_ports:
                self.peers.add(x)
        log_info(f'Получено {len(ip_ports)} пиров от трекера')