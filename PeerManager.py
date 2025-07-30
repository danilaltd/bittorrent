from BlockandPiece import BLOCK_SIZE, Status
from TrackersManager import TrackersManager
from peer import Peer, MAX_CONNECTION_ATTEMPTS
import threading
import socket
from Messages import *
from PieceInfo import PieceInfo
import time
import random
import struct
import os
from datetime import datetime
from logger import print_lock_stats
from rwlock import RWLock
import traceback
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
KEEPALIVE_INTERVAL = 60


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
        self._connected_peers: list[Peer] = []
        self._connected_peers_lock = RWLock("_connected_peers_lock")
        self.piece_manager = PieceInfo(tracker_obj.torrent_obj)
        self.torrent_completed = False
        self._optimistic_unchoke_peer: Peer = None
        self._running = True
        self._rarest_piece_min_heap: list[tuple[int, list[Peer]]] = []
        self._rarest_piece_min_heap_lock = threading.Lock()
        self.need_update_pieces = True
        self.notify = notify
        
        self._selector = selectors.DefaultSelector()
        
        self._send_bufs: dict[socket.socket, bytearray] = {}
        self._recv_bufs: dict[socket.socket, bytearray] = {}
        self._send_views: dict[socket.socket, memoryview] = {}
        self._send_queues: dict[socket.socket, queue.Queue] = {}
        self._handshake_completed: dict[socket.socket, bool] = {}
        self._sock_to_peer: dict[socket.socket, Peer] = {}
        
        self._receive_queue: queue.Queue = queue.Queue()
        self._peers_to_clear_queue: queue.Queue[Peer] = queue.Queue()
        
        self._periodic_piece_peers_thread = threading.Thread(target=self._periodic_piece_peers_update_enter)
        self._periodic_piece_peers_thread.start()
        
        self._sockets_thread = threading.Thread(target=self._socket_worker_loop_enter)
        self._sockets_thread.start()
        
        self._reader_thread = threading.Thread(target=self.read_continously_from_sock_enter)
        self._reader_thread.start()

    def _periodic_piece_peers_update_enter(self):
        self._periodic_piece_peers_update()
        
    def _socket_worker_loop_enter(self):
        self._socket_worker_loop()
        
    def read_continously_from_sock_enter(self):
        self.read_continously_from_sock()
    
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

    def _periodic_piece_peers_update(self):
        peer_to_clear:Peer | None = None
        peer_to_clear_getting_time = time.time()
        last_piece_update = time.time()
        last_optimistic_unchoke = time.time()
        last_peer_update = time.time()
        download_started = False
        PEER_CLEAR_TIMEOUT = 1
        last_reconnect_to_peers = time.time()
        while self._running:
            try:
                current_time = time.time()
                download_started = download_started or self.piece_manager.num_of_downloaded_pieces() + self.piece_manager.num_of_requested_pieces() >= 15
                update_pieces = (
                    self.need_update_pieces
                 or (current_time - last_piece_update >= MIN_PIECE_UPDATE_INTERVAL and not download_started) 
                 or (current_time - last_piece_update >= DEFAULT_PIECE_UPDATE_INTERVAL)
                )
                if update_pieces:
                    self.need_update_pieces = False
                    self.update_rarest_piece_min_heap()
                    last_piece_update = current_time
                    
                    
                update_peers = (
                    (current_time - last_peer_update >= MIN_PEER_UPDATE_INTERVAL and not download_started)
                    or (current_time - last_peer_update >= DEFAULT_PEER_UPDATE_INTERVAL)
                )
                if update_peers:
                    self._update_peers()
                    last_peer_update = current_time
                    
                reconnect_peers = current_time - last_reconnect_to_peers >= RECONNECT_INTERVAL
                if reconnect_peers:
                    self._reconnect_peers()
                    last_reconnect_to_peers = current_time
                    
                self.piece_manager.print_progress_bar_safe(
                    print_matrix=True,
                    peers=self._get_peers_for_progress()
                )

                self.monitor_peers_keepalives()
                
                if not peer_to_clear:
                    try:
                        peer_to_clear, sock = self._peers_to_clear_queue.get_nowait()
                        peer_to_clear_getting_time = time.time()
                    except queue.Empty:
                        peer_to_clear = None
                elif current_time - peer_to_clear_getting_time >= PEER_CLEAR_TIMEOUT:
                    with peer_to_clear.requested_blocks_per_piece_lock:
                        for piece_index, blocks in enumerate(peer_to_clear.requested_blocks_per_piece):
                            self.piece_manager.set_blocks_empty(piece_index, blocks, True)
                    # try:
                    self._sock_to_peer.pop(sock)
                    # except:
                        # pass
                    peer_to_clear = None
                    
                time.sleep(1)
            except Exception as e:
                log_error(f"Error in rarest piece update thread: {e} \n {traceback.format_exc()}")
                time.sleep(5)
    
    def update_rarest_piece_min_heap(self):
        res = self.getRarestPieceMinHeap()
        with self._rarest_piece_min_heap_lock:
            self._rarest_piece_min_heap = res
        self.notify()
      
    def monitor_peers_keepalives(self):
        connected_peers = self.get_connected_peers_copy()
        current_time = time.time()
        for peer in connected_peers:
            if current_time - peer.last_request_time >= KEEPALIVE_INTERVAL:
                self.send_data(peer, keep_alive().byteStringForKeepAlive())
                
        
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

    def _update_peers(self):
        try:
            with self._tracker_obj.peers_lock:
                peers_copy = self._tracker_obj.peers.copy()
            log_info(f"Got {len(peers_copy)} peers")
            for peer_ip_port in peers_copy:
                if not self._is_peer_in_peers_ip_port(peer_ip_port):
                    self._add_peer_ip_port(peer_ip_port)
                    peerObj = Peer(peer_ip_port, self.piece_manager.number_of_pieces, self.send_cancel)
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
                        if peer.bad_peer or peer.connecting or peer.connected:
                            continue
                        self.launch_thread(peer)
                    except Exception as e:
                        log_error(f"Error reconnecting to peer {peer.ip_port}", e)
        except Exception as e:
            log_error("Error in peer reconnection", e)

    def exitPeerThreads(self):
        self._running = False
        self.piece_manager.running = False
        with self._connected_peers_lock.read_access:
            connected_peers = self._connected_peers.copy()
        for peer in connected_peers:
            try:
                self._unregister_peer(peer.sock)
            except:
                pass
        self._clear_connected_peers()
        self._clear_peers()
        self._clear_peers_ip_port()
        
    def _socket_worker_loop(self):
        while self._running:
            # print(f"{self._get_connected_peers_count()} {len(self._selector.get_map())}")
            # print(f"rec: {self._receive_queue.qsize()}")
            try:
                if not self._selector.get_map():
                    time.sleep(0.05)
                    continue
                events = self._selector.select(timeout=1)
            except:
                continue
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
                    log_error(f"_socket_worker_loop_read: {e} {type(e)}, args: {e.args}, repr: {repr(e)}")
                
                try:
                    if mask & selectors.EVENT_WRITE:
                        self._handle_write(sock)
                except OSError as e:
                    ip_port = self._unregister_peer(sock)
                    log_error(f"write: closed {ip_port} {e}")
                    continue
                except Exception as e:
                    log_error(f"_socket_worker_loop_write: {e} {type(e)}, args: {e.args}, repr: {repr(e)}") #  \n{traceback.format_exc()}
            
    def _handle_write(self, sock: socket.socket):
        """Send as much data from send_buf and send_queue as socket allows."""
        send_queue = self._send_queues[sock]
        send_view = self._send_views[sock]
        send_buf = self._send_bufs[sock]
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
                print("BlockingIOError")
                pass
        # else:
            # print(f"{ip_port} empty")
            

    def _handle_read(self, sock: socket.socket):
        recv_buf = self._recv_bufs[sock]
        try:
            data = sock.recv(1048576)
            if not data:
                # connection closed by peer
                raise OSError("not sock.recv(1048576) -> connection closed")
            
        except BlockingIOError:
            print("no data")
            pass
        
        recv_buf.extend(data)

        offset = 0
        
        if not self._handshake_completed[sock]:
            if len(recv_buf) >= 68:
                handshake_msg = bytes(recv_buf[:68])
                self._receive_queue.put((sock, handshake_msg))
                offset = 68
                self._handshake_completed[sock] = True
        else:
            while len(recv_buf) - offset >= 4:
                length = struct.unpack_from('>I', recv_buf, offset)[0]
                if len(recv_buf) - offset < 4 + length:
                    break
                start = offset + 4
                end = start + length
                msg = bytes(recv_buf[offset:end])
                self._receive_queue.put((sock, msg))
                offset = end

        # remove parsed bytes
        if offset > 0:
            del recv_buf[:offset]

    def getMessage(self):
        try:
            data = self._receive_queue.get(timeout=1)   
            return data
        except: 
            return None

    def _get_peer_by_sock(self, sock) -> Peer:
        return self._sock_to_peer[sock]

    def read_continously_from_sock(self):
        try:
            while self._running:
                data = self.getMessage()
                if data:
                    sock, msg = data
                    peer = self._get_peer_by_sock(sock)
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
                                            
                                        if self.piece_manager.bit_field_ready:
                                            print(f"send bf to {peer.ip_port}")
                                            self.send_data(peer, Bitfield(self.piece_manager.get_my_bit_field()).byteStringForBitfield())
                                        self.send_data(peer, interested().byteStringForInterested())
                                        peer.am_interested = True
                                        log_info(f"Successfully connected to peer {peer.ip_port}")
                                        peer.connecting = False
                                except Exception as e:
                                    log_error(f"Handshake error for {peer.ip_port}", e)
                                    self._unregister_peer(peer.sock)
                                    peer.connecting = False
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
                                    peer.peer_interested = True
                                    log_info(f"Peer {peer.ip_port} is interested", flags = ['Reading'], name=f'{peer.ip_port}.log')
                                elif message_ID_u == 3:  # Not interested
                                    peer.peer_interested = False
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
                else:
                    log_info("Empty read queue")
                        
        except Exception as e:
            log_error(f"Fatal error in read thread. \n{traceback.format_exc()}", e, flags = ['Reading'])

        
    def launch_thread(self, peer: Peer):
        p = threading.Thread(target=self.MultiThreadedConnection, args=(peer,))
        p.daemon = True
        p.start()
    
    def _register_peer(self, peer: Peer):
        sock = peer.sock
        sock.setblocking(False)
        self._send_bufs[sock] = bytearray()
        self._recv_bufs[sock] = bytearray()
        self._send_views[sock] = None
        self._send_queues[sock] = queue.Queue()
        self._sock_to_peer[sock] = peer
        self._handshake_completed[sock] = False
        self._selector.register(sock, selectors.EVENT_READ | selectors.EVENT_WRITE)
        
    def _unregister_peer(self, sock: socket.socket):
        try:
            peer = self._sock_to_peer[sock]
        except KeyError:
            pass
        self._send_bufs.pop(sock, None)
        self._recv_bufs.pop(sock, None)
        self._send_views.pop(sock, None)
        self._send_queues.pop(sock, None)
        self._handshake_completed.pop(sock, None)
        
        
        try:
            self._selector.unregister(sock)
        except Exception:
            pass 
        if peer:
            if self._is_peer_connected(peer):
                    self._remove_connected_peer(peer)
            self._peers_to_clear_queue.put((peer, peer.sock))
            peer.close()
            ip_port = peer.ip_port
            return ip_port
        return None
    
    def MultiThreadedConnection(self, peer:Peer):
        # if peer.ip != '5.79.98.162' and peer.ip != '90.240.225.228':
            # return
        log_info(f"MultiThreadedConnection in {peer.ip}")
        if peer.connection_attempts >= MAX_CONNECTION_ATTEMPTS:
            log_info(f"Max connection attempts reached for {peer.ip_port}")
            return
        peer.connecting = True
        try:
            # Validate peer address before connecting
            if not peer.ip_port or not isinstance(peer.ip_port[1], int) or peer.ip_port[1] <= 0:
                log_error(f"Invalid peer address {peer.ip_port}")
                return
                
            
            if not peer.connect_to_peer():
                peer.bad_peer = True
                peer.connecting = False
                return
                
            self._register_peer(peer)
            if not self._is_peer_in_peers_ip_port((peer.ip, peer.port)):
                self._add_peer_ip_port((peer.ip, peer.port))
            handshake = Handshake(self._tracker_obj.torrent_obj.peer_id, self._tracker_obj.torrent_obj.info_hash)
            
            handshake_bytes = handshake.getHandshakeBytes()
            self.send_data(peer, handshake_bytes)
            peer.handshake_sent = True
                
            
        except Exception as e:
            log_error(f"Connection error for {peer.ip_port} \nTraceback:\n{traceback.format_exc()}", e)

    def send_data(self, peer: Peer, data):
        if peer.connected:
            peer.last_request_time = time.time()
            self._send_queues[peer.sock].put(data)
        

    def prefetch_next_blocks(self, piece_index, peer: Peer):
        """Request next blocks from the same piece following BitTorrent protocol"""
        sent = False
        for block_index in self.piece_manager.get_empty_blocks(piece_index):
            try:
                current_time = time.time()
                self.send_data(peer, request(piece_index, block_index * BLOCK_SIZE, self.piece_manager.get_block_size(piece_index, block_index)).byteStringForRequest())
                self.piece_manager.update_block_status_safe(piece_index, block_index, Status.REQUESTED, last_requested=current_time, requested_by = peer)                            
                sent = True
            except Exception as e:
                log_error(f"Error requesting next block: {piece_index}:{block_index}; {e}, \nTraceback:\n{traceback.format_exc()}")
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
                        self.send_data(self._optimistic_unchoke_peer, unchoke_msg.byteStringForUnchoke())
                        self._optimistic_unchoke_peer.am_choking = False
                    except Exception as e:
                        log_error(f"Error sending optimistic unchoke to {self._optimistic_unchoke_peer.ip_port}", e)
                
                # Select new optimistic peer
                self._optimistic_unchoke_peer = random.choice(choked_peers)
                try:
                    unchoke_msg = unchoke()
                    self.send_data(self._optimistic_unchoke_peer, unchoke_msg.byteStringForUnchoke())
                    self._optimistic_unchoke_peer.am_choking = False
                    log_info(f"Optimistic unchoke for {self._optimistic_unchoke_peer.ip_port}")
                except Exception as e:
                    log_error(f"Error sending optimistic unchoke to {self._optimistic_unchoke_peer.ip_port}", e)
            
            self._last_optimistic_unchoke = current_time

    def send_cancel(self, peer: Peer, piece_index, block_index):
        self.send_data(peer, cancel(piece_index, block_index * BLOCK_SIZE, self.piece_manager.get_block_size(piece_index, block_index)).byteStringForCancel())

    def get_rarest_piece_min_heap_copy(self):
        with self._rarest_piece_min_heap_lock:
            return self._rarest_piece_min_heap.copy()

    def get_connected_peers_copy(self):
        with self._connected_peers_lock.read_access:
            return self._connected_peers.copy()

    def _get_peers_for_progress(self):
        with self._peers_lock.read_access:
            return self._peers.copy()
    
    def print_lock_statistics(self):
        """Выводит статистику использования locks для этого объекта"""
        print_lock_stats()