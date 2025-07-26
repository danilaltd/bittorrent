from math import pi
from BlockandPiece import BLOCK_SIZE, Status
from peer import Peer
from Messages import Handshake
from TrackersManager import TrackersManager
from peer import Peer, MAX_CONNECTION_ATTEMPTS
import threading
import socket
from Messages import *
from PieceInfo import PieceInfo
import socket
import time
import random
import struct
import os
from datetime import datetime
from logger import timed_lock, print_lock_stats
from rwlock import RWLock
import traceback
from logger import Logger
from itertools import groupby
import selectors
import queue


MAX_CONNECTED_PEER = 50
MIN_PEER_UPDATE_INTERVAL = 3
DEFAULT_PEER_UPDATE_INTERVAL = 3
MIN_PIECE_UPDATE_INTERVAL = 3
DEFAULT_PIECE_UPDATE_INTERVAL = 3
RECONNECT_INTERVAL = 10
OPTIMISTIC_UNCHOKE_INTERVAL = 30

def log_error(msg, exc=None, flags = None, name = ''):
    if flags is None:
        flags = []
    flags.insert(0, 'ERROR')
    flags.insert(0, f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f'[{timestamp}] {msg}\n'
    if exc is not None:
        if isinstance(exc, ConnectionResetError) or ('10054' in str(exc)):
            flags.append('Network')
            log_entry += f'Пир разорвал соединение (обычно для BitTorrent): {msg} — {exc}'
        else:
            log_entry = f'{msg}: {exc}'
    else:
        log_entry = f'{msg}'
    log_entry += '\n'
    res = ''
    for flag in flags:
        res += f"[{flag}]"
    res += ' '    
    res += log_entry    
    if name:
        path = os.path.join('logs', 'peermanager')
    else:
        name = 'peerManager.log'
        path = 'logs'
        
    with open(os.path.join(f'{path}', f"{name}"), 'a', encoding='utf-8') as f:
        f.write(res)

def log_info(msg, flags = None, name = ''):
    if flags is None:
        flags = []
    flags.insert(0, f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_entry = f'{msg}\n'
    res = ''
    for flag in flags:
        res += f"[{flag}]"
    res += ' '    
    res += log_entry    
    if name:
        path = os.path.join('logs', 'peermanager')
    else:
        name = 'peerManager.log'
        path = 'logs'
        
    with open(os.path.join(f'{path}', f"{name}"), 'a', encoding='utf-8') as f:
        f.write(res)

class PeerManager:
    def __init__(self, tracker_obj: TrackersManager, notify):
        self._tracker_obj: TrackersManager = tracker_obj
        self._peers: list[Peer] = []
        self._peers_lock = RWLock("_peers_lock")
        self._peers_ip_port: list[tuple[str, str]] = []
        self._peers_ip_port_lock = RWLock("_peers_ip_port_lock")
        self._peers_ip_port_to_Peer: dict[tuple[str, int], Peer] = {}
        self._peers_ip_port_to_Peer_lock = RWLock("_peers_ip_port_to_Peer_lock")
        self._connected_peers: list[Peer] = []
        self._connected_peers_lock = RWLock("_connected_peers_lock")
        self.piece_manager = PieceInfo(tracker_obj.torrent_obj)
        self.torrent_completed = False
        self._last_optimistic_unchoke = time.time()
        self._optimistic_unchoke_peer: Peer = None
        self._last_peer_update = time.time()
        self._last_piece_update = time.time()
        self._last_reconnect = time.time()
        self._running = True
        self._rarest_piece_min_heap: list[tuple[int, list[Peer]]] = []
        self._rarest_piece_min_heap_lock = threading.Lock()
        self._download_started = False
        
        self.need_update_pieces = True
        self.notify = notify
        
        self._rarest_piece_thread = threading.Thread(target=self._periodic_rarest_piece_update)
        self._rarest_piece_thread.daemon = True
        self._rarest_piece_thread.start()
        self._peer_update_thread = threading.Thread(target=self._periodic_peer_update)
        self._peer_update_thread.daemon = True
        self._peer_update_thread.start()
        
        self.selector = selectors.DefaultSelector()
        
        self.send_bufs: dict[tuple[str, int] | tuple[str, int, int, int], bytearray] = {}
        self.send_views: dict[tuple[str, int] | tuple[str, int, int, int], memoryview] = {}
        self.send_queues: dict[tuple[str, int] | tuple[str, int, int, int], queue.Queue] = {}
        self.receive_queue: queue.Queue = queue.Queue()
        self.recv_bufs: dict[tuple[str, int] | tuple[str, int, int, int], bytearray] = {}
        self.sock_to_peer: dict[socket.socket, Peer] = {}
        self._handshake_completed: dict[tuple[str, int] | tuple[str, int, int, int], bool] = {}
        
        self._sockets_thread = threading.Thread(target=self._socket_worker_loop)
        self._sockets_thread.daemon = True
        self._sockets_thread.start()
        
        self._reader_thread = threading.Thread(target=self.read_continously_from_sock)
        self._reader_thread.daemon = True
        self._reader_thread.start()

    def _is_peer_in_peers(self, peer: Peer):
        with self._peers_lock.read_access:
            return peer in self._peers
    
    def _add_peer(self, peer: Peer):
        with self._peers_lock.write_access:
            self._peers.append(peer)
            
    def _clear_peers(self):
        with self._peers_lock.write_access:
            self._peers.clear()
            
    def _is_peer_in_peers_ip_port(self, peer: tuple[str, str]):
        with self._peers_ip_port_lock.read_access:
            return peer in self._peers_ip_port
    
    def _add_peer_ip_port(self, peer: tuple[str, str]):
        with self._peers_ip_port_lock.write_access:
            self._peers_ip_port.append(peer)
            
    def _clear_peers_ip_port(self):
        with self._peers_ip_port_lock.write_access:
            self._peers_ip_port.clear()
            
    def _add_connected_peer(self, peer):
        with self._connected_peers_lock.write_access:
            self._connected_peers.append(peer)
        self.request_piece_update()


    def _remove_connected_peer(self, peer):
        with self._connected_peers_lock.write_access:
            self._connected_peers.remove(peer)
        self.request_piece_update()

    def _clear_connected_peers(self):
        with self._connected_peers_lock.write_access:
            self._connected_peers.clear()
        self.request_piece_update()


    def _get_connected_peers_count(self):
        with self._connected_peers_lock.read_access:
            return len(self._connected_peers)

    def _is_peer_connected(self, peer):
        with self._connected_peers_lock.read_access:
            return peer in self._connected_peers

    def set_Peer_to_ip_port(self, ip_port, peer: Peer):
        with self._peers_ip_port_to_Peer_lock.write_access:
            self._peers_ip_port_to_Peer[ip_port] = peer

    def _periodic_rarest_piece_update(self):
        while self._running:
            try:
                current_time = time.time()
                self._download_started = self._download_started or self.piece_manager.num_of_downloaded_pieces() + self.piece_manager.num_of_requested_pieces() >= 15
                need_update = (
                    self.need_update_pieces
                 or (current_time - self._last_piece_update >= MIN_PIECE_UPDATE_INTERVAL and not self._download_started) 
                 or (current_time - self._last_piece_update >= DEFAULT_PIECE_UPDATE_INTERVAL)
                )
                if need_update:
                    self.need_update_pieces = False
                    self.update_rarest_piece_min_heap(current_time)
                time.sleep(1)
            except Exception as e:
                log_error("Error in rarest piece update thread", e)
                time.sleep(5)
                
    def update_rarest_piece_min_heap(self, current_time):
        self._last_piece_update = current_time
        res = self.getRarestPieceMinHeap()
        with self._rarest_piece_min_heap_lock:
            self._rarest_piece_min_heap = res
        self.notify()
        
    def request_piece_update(self):
        self.need_update_pieces = True
        
        
    def getRarestPieceMinHeap(self):
        piece_listOfpeers = {}
        for i in range(self.piece_manager.number_of_pieces):
            piece_listOfpeers[i] = []
        
        with self._connected_peers_lock.read_access:
            for index in range(self.piece_manager.number_of_pieces):
                if not self.piece_manager.is_piece_complete(index):
                    for peer in self._connected_peers:
                        if peer.connected and peer.peer_choking == 0 and peer.is_bit_set_in_bit_field(index):
                            piece_listOfpeers[index].append(peer)
        filtered = {k: v for k, v in piece_listOfpeers.items() if len(v) > 0}
        piece_listOfpeers = (sorted(filtered.items(), key = lambda kv:(len(kv[1]), kv[0]))) #Сортировка кусков по числу пиров, у которых они есть
        result: list[tuple[int, list[Peer]]] = [] #Перемешивание в группах одинаковой редкости
        for _, group in groupby(piece_listOfpeers, key=lambda kv: len(kv[1])):
            group_list = list(group)
            random.shuffle(group_list)
            result.extend(group_list)
        return result

    def _periodic_peer_update(self):
        while self._running:
            try:
                current_time = time.time()
                if current_time - self._last_peer_update >= MIN_PEER_UPDATE_INTERVAL:
                    update = current_time - self._last_peer_update >= DEFAULT_PEER_UPDATE_INTERVAL
                    if not update and not self._download_started:
                        downloaded = self.piece_manager.num_of_downloaded_pieces()
                        requested = self.piece_manager.num_of_requested_pieces()
                        update = downloaded + requested < 15
                        self._download_started = downloaded + requested >= 15
                    if update:
                        self._update_peers(current_time)
                if current_time - self._last_reconnect >= RECONNECT_INTERVAL:
                    self._reconnect_peers()
                    self._last_reconnect = current_time
                time.sleep(1)
            except Exception as e:
                log_error("Error in peer update thread", e)
                time.sleep(5)

    def _update_peers(self, current_time):
        try:
            self._last_peer_update = current_time
            with self._tracker_obj.peers_lock:
                peers_copy = self._tracker_obj.peers.copy()
            log_info(f"Got {len(peers_copy)} peers")
            for peer_ip_port in peers_copy:
                if not self._is_peer_in_peers_ip_port(peer_ip_port):
                    self._add_peer_ip_port(peer_ip_port)
                    peerObj = Peer(peer_ip_port, self.piece_manager.number_of_pieces)
                    self._add_peer(peerObj)
                    self.launch_thread(peerObj)
        except Exception as e:
            log_error(f"Error updating peer list, \nTraceback:\n{traceback.format_exc()}", e)

    def _reconnect_peers(self):
        try:
            with self._connected_peers_lock.read_access:
                with self._peers_lock.read_access:
                    disconnected_peers = [p for p in self._peers if not (p.connected and p in self._connected_peers) and p.connection_attempts < MAX_CONNECTION_ATTEMPTS]
            if disconnected_peers:
                log_info(f"Attempting to reconnect to {len(disconnected_peers)} peers...")
                sorted_peers = disconnected_peers
                for peer in sorted_peers:
                    if self._get_connected_peers_count() >= MAX_CONNECTED_PEER:
                        break
                    try:
                        if peer.bad_peer or peer.connecting:
                            continue
                        self.launch_thread(peer)
                    except Exception as e:
                        log_error(f"Error reconnecting to peer {peer.ip_port}", e)
        except Exception as e:
            log_error("Error in peer reconnection", e)

    def exitPeerThreads(self):
        self._running = False
        if self._peer_update_thread.is_alive():
            self._peer_update_thread.join(timeout=5)
        with self._connected_peers_lock.read_access:
            for peer in self._connected_peers:
                try:
                    peer.close()
                except:
                    pass
        self._clear_connected_peers()
        self._clear_peers()
        self._clear_peers_ip_port()
        
    def _socket_worker_loop(self):
        while self._running:
            # print(f"{self._get_connected_peers_count()} {len(self.selector.get_map())}")
            # print(f"rec: {self.receive_queue.qsize()}")
            events = self.selector.select(timeout=1)
            for key, mask in events:
                sock = key.fileobj
                try:
                    if mask & selectors.EVENT_READ:
                        self._handle_read(sock)
                except OSError as e:
                    ip_port = self._unregister_peer(sock)
                    log_error(f"read: closed {ip_port} {e}")
                    continue
                except Exception as e:
                    log_error(f"_socket_worker_loop_read: {e}")
                
                try:
                    if mask & selectors.EVENT_WRITE:
                        self._handle_write(sock)
                except OSError as e:
                    ip_port = self._unregister_peer(sock)
                    log_error(f"write: closed {ip_port} {e}")
                    continue
                except Exception as e:
                    log_error(f"_socket_worker_loop_write: {e}") #  \n{traceback.format_exc()}
            
    def _handle_write(self, sock: socket.socket):
        """Send as much data from send_buf and send_queue as socket allows."""
        # Refill send_buf if empty and no current view
        ip_port = sock.getpeername()
        send_queue = self.send_queues[ip_port]
        send_view = self.send_views[ip_port]
        send_buf = self.send_bufs[ip_port]
        if send_view is None:
            # i = 0
            # while not send_buf:
            while True:
                try:
                    chunk = send_queue.get_nowait()
                    # i += 1
                    send_buf.extend(chunk)
                except queue.Empty:
                    break
            # print(f"{ip_port}: send {i}/{send_queue.qsize()}")
            if send_buf:
                send_view = memoryview(send_buf)

        if send_view:
            try:
                sent = sock.send(send_view)
                if sent > 0:
                    send_view = send_view[sent:]
                    if len(send_view) == 0:
                        send_view = None
                        send_buf.clear()
                else:
                    raise OSError(f"sent == 0")
            except BlockingIOError:
                log_error("BlockingIOError")
                pass

    def _handle_read(self, sock: socket.socket):
        ip_port = sock.getpeername()
        recv_buf = self.recv_bufs[ip_port]
        try:
            data = sock.recv(1048576)
            if not data:
                # connection closed by peer
                raise OSError("not sock.recv(8192) -> connection closed")
            recv_buf.extend(data)

            offset = 0
            
            if not self._handshake_completed[ip_port]:
                if len(recv_buf) >= 68:
                    handshake_msg = bytes(recv_buf[:68])
                    self.receive_queue.put((ip_port, handshake_msg))
                    offset = 68
                    self._handshake_completed[ip_port] = True
            else:
                while len(recv_buf) - offset >= 4:
                    length = struct.unpack_from('>I', recv_buf, offset)[0]
                    if len(recv_buf) - offset < 4 + length:
                        break
                    start = offset + 4
                    end = start + length
                    msg = bytes(recv_buf[offset:end])
                    self.receive_queue.put((ip_port, msg))
                    offset = end

            # remove parsed bytes
            if offset > 0:
                del recv_buf[:offset]

        except BlockingIOError:
            print("no data")
            pass

    def getMessage(self):
        try:
            data = self.receive_queue.get()   
            return data
        except: 
            return None

    def get_peer_by_ip_port(self, ip_port) -> Peer:
        with self._peers_ip_port_to_Peer_lock.read_access:
            return self._peers_ip_port_to_Peer[ip_port]

    def read_continously_from_sock(self):
        try:
            while True:
                data = self.getMessage()
                if data:
                    ip_port, msg = data
                    peer = self.get_peer_by_ip_port(ip_port)
                    if msg and peer:
                        try:
                            message_length = msg[:4]
                            
                            if not message_length:
                                log_error(f"Invalid message length from {peer.ip_port}: {len(message_length)}", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                continue
                                
                            message_length = struct.unpack(">I", message_length)[0]
                            # log_info(f"{peer.ip_port}: got message with {message_length} bytes", flags = ['Reading'], name=f'{peer.ip_port}.log')
                            if message_length == 0:
                                continue
                            
                            if peer.connecting:
                                try:
                                    if len(msg) == 68:
                                        if msg[0] != 19 or msg[1:20] != b'BitTorrent protocol':
                                            raise Exception(f"Invalid handshake protocol")
                                
                                        received_info_hash = msg[28:48]
                                        if received_info_hash != self._tracker_obj.torrent_obj.info_hash:
                                            raise Exception(f"Info hash mismatch")
                                
                                        peer.handshake_received = True
                                        log_info(f"Handshake successful with {peer.ip_port}")
                                        
                                        if not self._is_peer_connected(peer):    
                                            self._add_connected_peer(peer)
                                            
                                        self.send_data(peer.ip_port, interested().byteStringForInterested())
                                        peer.am_interested = 1
                                        log_info(f"Successfully connected to peer {peer.ip_port}")
                                        peer.connecting = False
                                except Exception as e:
                                    log_error(f"Handshake error for {peer.ip_port}", e)
                                    ip_port = self._unregister_peer(peer.sock)
                                    peer.bad_peer = True
                            else:
                                message_ID = msg[4:5]
                                if not message_ID:
                                    log_error(f"Invalid message ID from {peer.ip_port}", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                    continue
                                    
                                message_ID_u = struct.unpack(">B", message_ID)[0]
                                
                                if message_ID_u == 0:  # Choke
                                    peer.peer_choking = True
                                    self.request_piece_update()
                                    log_info(f"Peer {peer.ip_port} choked us", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                elif message_ID_u == 1:  # Unchoke
                                    peer.peer_choking = False
                                    self.request_piece_update()
                                    log_info(f"Peer {peer.ip_port} unchoked us", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                elif message_ID_u == 2:  # Interested
                                    peer.peer_interested = 1
                                    log_info(f"Peer {peer.ip_port} is interested", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                elif message_ID_u == 3:  # Not interested
                                    peer.peer_interested = 0
                                    log_info(f"Peer {peer.ip_port} is not interested", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                elif message_ID_u == 4:  # Have
                                    if message_length - 1 == 4:
                                        piece_index = msg[5:9]
                                        if piece_index:
                                            piece_index = struct.unpack(">I", piece_index)[0]
                                        else:
                                            log_error(f"{peer.ip_port} 'have' message: couldn't read piece_index", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                            continue
                                        if peer.set_bit_in_bit_field(piece_index):
                                            log_info(f"{peer.ip_port} 'have' message received for piece {piece_index}", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                            self.request_piece_update()
                                        else:
                                            log_info(f"{peer.ip_port} 'have' message: piece_index ({piece_index}, more or eq than {len(peer.bit_field)}) or already set", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                    else:
                                        log_error(f"{peer.ip_port} 'have' message: wrong length ({message_length}, not 5)", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                elif message_ID_u == 5:  # Bitfield
                                    bitfield_length = message_length - 1
                                    
                                    bitfield_data = msg[5:5 + bitfield_length]
                                    if bitfield_data:
                                        if len(bitfield_data) == bitfield_length:
                                            peer.set_bit_field(bitfield_data)
                                            self.request_piece_update()
                                            log_info(f"Received valid bitfield from {peer.ip_port}: length is {len(bitfield_data)}", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                        else:
                                            log_error(f"Received invalid bitfield from {peer.ip_port}: length is {len(bitfield_data)}, need {bitfield_length}", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                    else:
                                        log_error(f"bitfield: couldn't read bitfield_data", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                elif message_ID_u == 6:  # Request
                                    # log_info(f"Received request from {peer.ip_port}", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                    data = msg[5:17]
                                    if data:
                                        pass
                                    else:
                                        log_error(f"Request: couldn't read request data", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                        
                                    if len(data) < 12:
                                        continue
                                elif message_ID_u == 7:  # Piece
                                    try:
                                        piece_index_b = msg[5:9]
                                        if not piece_index_b:
                                            log_error(f"Invalid piece_index_b from {peer.ip_port}: {piece_index_b}", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                            continue
                                        block_offset_b = msg[9:13]
                                        if not block_offset_b:
                                            log_error(f"Invalid piece_index_b from {peer.ip_port}: {block_offset_b}", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                            continue
                                        piece_index = struct.unpack(">I", piece_index_b)[0]
                                        block_offset = struct.unpack(">I", block_offset_b)[0]
                                        block_index = block_offset // BLOCK_SIZE
                                        
                                        block_data = msg[13:message_length + 4]
                                        if block_data is None:
                                            log_error(f"Invalid piece {piece_index} from {peer.ip_port}: couldn't read data", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                            continue
                                            

                                        if self.piece_manager.update_block_status_safe(piece_index, block_index, Status.DOWNLOADED, block_data, requested_by=peer):
                                            pass
                                    except struct.error as e:
                                        log_error(f"Error unpacking piece message from {peer.ip_port}", e, flags = ['Reading'], name=f'{peer.ip_port}.log')
                                        continue
                                    except Exception as e:
                                        log_error(f"Error processing piece message from {peer.ip_port}", e)
                                        continue
                                else:
                                    log_error(f"Unsupported message from {peer.ip_port}: message length: {message_length}, id: {message_ID_u}", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                
                                            
                        except Exception as e:
                            log_error(f"Error reading from {peer.ip_port}", e, flags = ['Reading'], name=f'{peer.ip_port}.log')
                            break
                # else:
                    # log_error("empty")
                        
        except Exception as e:
            log_error(f"Fatal error in read thread for {ip_port} {traceback.format_exc()}", e, flags = ['Reading'], name=f'{ip_port}.log')

        
    def launch_thread(self, peer: Peer):
        p = threading.Thread(target=self.MultiThreadedConnection, args=(peer,))
        p.daemon = True
        p.start()
    
    def _register_peer(self, peer: Peer):
        sock = peer.sock
        sock.setblocking(False)
        ip_port = sock.getpeername()
        self.send_bufs[ip_port] = bytearray()
        self.send_views[ip_port] = None
        self.send_queues[ip_port] = queue.Queue()
        self.recv_bufs[ip_port] = bytearray()
        self.sock_to_peer[sock] = peer
        self._handshake_completed[ip_port] = False
        self.selector.register(sock, selectors.EVENT_READ | selectors.EVENT_WRITE)
        
    def _unregister_peer(self, sock: socket.socket):
        try:
            peer = self.sock_to_peer.pop(sock)
        except KeyError:
            return
        ip_port = peer.ip_port
        self.send_bufs.pop(ip_port, None)
        self.send_views.pop(ip_port, None)
        self.send_queues.pop(ip_port, None)
        self.recv_bufs.pop(ip_port, None)
        self._handshake_completed.pop(ip_port, None)
        
        if self._is_peer_connected(peer):
            self._remove_connected_peer(peer)

        try:
            self.selector.unregister(sock)
        except Exception:
            pass 

        try:
            peer.close()
        except Exception:
            pass
        
        return ip_port
    
    def MultiThreadedConnection(self, peer:Peer):
        # if peer.ip != '5.79.98.162' and peer.ip != '90.240.225.228':
            # return
        log_info(f"MultiThreadedConnection in {peer.ip}")
        if peer.connection_attempts >= MAX_CONNECTION_ATTEMPTS:
            log_info(f"Max connection attempts reached for {peer.ip_port}")
            return
        peer.connecting = True
        sock = None
        try:
            # Validate peer address before connecting
            if not peer.ip_port or not isinstance(peer.ip_port[1], int) or peer.ip_port[1] <= 0:
                log_error(f"Invalid peer address {peer.ip_port}")
                return
                
            sock = peer.connect_to_peer()
            if sock is None:
                peer.bad_peer = True
                return
                
            self._register_peer(peer)
            if not self._is_peer_in_peers(peer):
                self._add_peer(peer)
            if not self._is_peer_in_peers_ip_port((peer.ip, peer.port)):
                self._add_peer_ip_port((peer.ip, peer.port))
            self.set_Peer_to_ip_port(peer.ip_port, peer)
            handshake = Handshake(self._tracker_obj.torrent_obj.peer_id, self._tracker_obj.torrent_obj.info_hash)
            
            handshake_bytes = handshake.getHandshakeBytes()
            self.send_data(peer.ip_port, handshake_bytes)
            peer.handshake_sent = True
                
            
        except Exception as e:
            log_error(f"Connection error for {peer.ip_port} \nTraceback:\n{traceback.format_exc()}", e)

    def send_data(self, ip_port, data):
        self.send_queues[ip_port].put(data)
        # print(self.send_queues[ip_port].qsize())
        

    def prefetch_next_blocks(self, piece_index, peer: Peer):
        """Request next blocks from the same piece following BitTorrent protocol"""
        sent = False
        for block_index in self.piece_manager.get_empty_blocks(piece_index):
            try:
                current_time = time.time()
                self.send_data(peer.ip_port, request(piece_index, block_index * BLOCK_SIZE, self.piece_manager.get_block_size(piece_index, block_index)).byteStringForRequest())
                self.piece_manager.update_block_status_safe(piece_index, block_index, Status.REQUESTED, last_requested=current_time, requested_by = peer)                            
                sent = True
                    
            except Exception as e:
                log_error(f"Error requesting next block: {piece_index}:{block_index}", e)
                raise
            
        return sent

    def update_optimistic_unchoke(self):
        """Update optimistic unchoke every 30 seconds"""
        current_time = time.time()
        if current_time - self._last_optimistic_unchoke >= OPTIMISTIC_UNCHOKE_INTERVAL:
            # Find a random choked peer that we're interested in
            with self._connected_peers_lock.read_access:
                choked_peers = [p for p in self._connected_peers if p.peer_choking and p.am_interested]
            if choked_peers:
                # Unchoke the previous optimistic peer if it exists
                if self._optimistic_unchoke_peer and self._is_peer_connected(self._optimistic_unchoke_peer):
                    try:
                        unchoke_msg = unchoke()
                        self.send_data(self._optimistic_unchoke_peer.ip_port, unchoke_msg.byteStringForUnchoke())
                        self._optimistic_unchoke_peer.am_choking = 0
                    except Exception as e:
                        log_error(f"Error sending optimistic unchoke to {self._optimistic_unchoke_peer.ip_port}", e)
                
                # Select new optimistic peer
                self._optimistic_unchoke_peer = random.choice(choked_peers)
                try:
                    unchoke_msg = unchoke()
                    self.send_data(self._optimistic_unchoke_peer.ip_port, unchoke_msg.byteStringForUnchoke())
                    self._optimistic_unchoke_peer.am_choking = 0
                    log_info(f"Optimistic unchoke for {self._optimistic_unchoke_peer.ip_port}")
                except Exception as e:
                    log_error(f"Error sending optimistic unchoke to {self._optimistic_unchoke_peer.ip_port}", e)
            
            self._last_optimistic_unchoke = current_time

    def get_rarest_piece_min_heap_copy(self):
        with self._rarest_piece_min_heap_lock:
            return self._rarest_piece_min_heap.copy()

    def get_connected_peers_for_stats(self):
        with self._connected_peers_lock.read_access:
            return self._connected_peers.copy()

    def get_peers_for_progress(self):
        with self._peers_lock.read_access:
            return self._peers.copy()

    def print_lock_statistics(self):
        """Выводит статистику использования locks для этого объекта"""
        print_lock_stats()