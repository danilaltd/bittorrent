from math import pi
from BlockandPiece import BLOCK_SIZE, Status, Block
from peer import Peer
from Messages import Handshake
from tracker import Tracker
from peer import Peer, MAX_CONNECTION_ATTEMPTS
import threading
import socket
from Messages import *
from PieceInfo import PieceInfo
import socket
import time
import random
import struct
import bitstring
import os
from datetime import datetime
from logger import timed_lock, lock_decorator, print_lock_stats
from rwlock import RWLock
import traceback
from logger import Logger
from itertools import groupby


MAX_CONNECTED_PEER = 50
MIN_PEER_UPDATE_INTERVAL = 3
DEFAULT_PEER_UPDATE_INTERVAL = 30
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
    def __init__(self, tracker_obj: Tracker):
        self._tracker_obj: Tracker = tracker_obj
        self._peers: list[Peer] = []
        self._peers_lock = RWLock("_peers_lock")
        self._peers_ip_port: list[tuple[str, str]] = []
        self._peers_ip_port_lock = RWLock("_peers_ip_port_lock")
        self._connected_peers: list[Peer] = []
        self._connected_peers_lock = RWLock("_connected_peers_lock")
        self.piece_manager = PieceInfo(tracker_obj.torrent_obj)
        self.torrent_completed = False
        self._last_optimistic_unchoke = time.time()
        self._last_optimistic_unchoke_lock = RWLock("_last_optimistic_unchoke_lock")
        self._optimistic_unchoke_peer: Peer = None
        self._optimistic_unchoke_peer_lock = RWLock("_optimistic_unchoke_peer_lock")
        self._last_peer_update = time.time()
        self._last_piece_update = time.time()
        self._last_reconnect = time.time()
        self._running = True
        self._rarest_piece_min_heap = self.getRarestPieceMinHeap()
        self._rarest_piece_min_heap_lock = RWLock("_rarest_piece_min_heap_lock")
        self._download_started = False
        self._rarest_piece_thread = threading.Thread(target=self._periodic_rarest_piece_update)
        self._rarest_piece_thread.daemon = True
        self._rarest_piece_thread.start()
        self._peer_update_thread = threading.Thread(target=self._periodic_peer_update)
        self._peer_update_thread.daemon = True
        self._peer_update_thread.start()

    @property
    def last_optimistic_unchoke(self):
        with timed_lock(self._last_optimistic_unchoke_lock.read_access, "_last_optimistic_unchoke_lock.read_access"):
            return self._last_optimistic_unchoke
    @last_optimistic_unchoke.setter
    def last_optimistic_unchoke(self, value):
        with timed_lock(self._last_optimistic_unchoke_lock.write_access, "_last_optimistic_unchoke_lock.write_access"):
            self._last_optimistic_unchoke = value

    @property
    def optimistic_unchoke_peer(self):
        with timed_lock(self._optimistic_unchoke_peer_lock.read_access, "_optimistic_unchoke_peer_lock.read_access"):
            return self._optimistic_unchoke_peer
    @optimistic_unchoke_peer.setter
    def optimistic_unchoke_peer(self, value):
        with timed_lock(self._optimistic_unchoke_peer_lock.write_access, "_optimistic_unchoke_peer_lock.write_access"):
            self._optimistic_unchoke_peer = value

    @property
    def rarest_piece_min_heap(self):
        with timed_lock(self._rarest_piece_min_heap_lock.read_access, "_rarest_piece_min_heap_lock.read_access"):
            return self._rarest_piece_min_heap
    @rarest_piece_min_heap.setter
    def rarest_piece_min_heap(self, value):
        with timed_lock(self._rarest_piece_min_heap_lock.write_access, "_rarest_piece_min_heap_lock.write_access"):
            self._rarest_piece_min_heap = value


    def _is_peer_in_peers(self, peer: Peer):
        with timed_lock(self._peers_lock.read_access, "_peers_lock.read_access"):
            return peer in self._peers
    
    def _add_peer(self, peer: Peer):
        with timed_lock(self._peers_lock.write_access, "_peers_lock.write_access"):
            self._peers.append(peer)
            
    def _clear_peers(self):
        with timed_lock(self._peers_lock.write_access, "_peers_lock.write_access"):
            self._peers.clear()
            
    def _is_peer_in_peers_ip_port(self, peer: tuple[str, str]):
        with timed_lock(self._peers_ip_port_lock.read_access, "_peers_ip_port.read_access"):
            return peer in self._peers_ip_port
    
    def _add_peer_ip_port(self, peer: tuple[str, str]):
        with timed_lock(self._peers_ip_port_lock.write_access, "_peers_ip_port.write_access"):
            self._peers_ip_port.append(peer)
            
    def _clear_peers_ip_port(self):
        with timed_lock(self._peers_ip_port_lock.write_access, "_peers_ip_port.write_access"):
            self._peers_ip_port.clear()
            
    def _add_connected_peer(self, peer):
        with timed_lock(self._connected_peers_lock.write_access, "_connected_peers_lock.write_access"):
            self._connected_peers.append(peer)
        self.update_rarest_piece_min_heap(time.time())

    def _remove_connected_peer(self, peer):
        with timed_lock(self._connected_peers_lock.write_access, "_connected_peers_lock.write_access"):
            self._connected_peers.remove(peer)
        self.update_rarest_piece_min_heap(time.time())

    def _clear_connected_peers(self):
        with timed_lock(self._connected_peers_lock.write_access, "_connected_peers_lock.write_access"):
            self._connected_peers.clear()
        self.update_rarest_piece_min_heap(time.time())

    def _get_connected_peers_count(self):
        with timed_lock(self._connected_peers_lock.read_access, "_connected_peers_lock.read_access"):
            return len(self._connected_peers)

    def _is_peer_connected(self, peer):
        with timed_lock(self._connected_peers_lock.read_access, "_connected_peers_lock.read_access"):
            return peer in self._connected_peers

    def _periodic_rarest_piece_update(self):
        while self._running:
            try:
                current_time = time.time()
                if current_time - self._last_piece_update >= MIN_PIECE_UPDATE_INTERVAL:
                    update = current_time - self._last_piece_update >= DEFAULT_PIECE_UPDATE_INTERVAL
                    if not update and not self._download_started:
                        downloaded = self.piece_manager.num_of_downloaded_pieces()
                        requested = self.piece_manager.num_of_requested_pieces()
                        update = downloaded + requested < 15
                        self._download_started = downloaded + requested >= 15
                    if update:
                        self.update_rarest_piece_min_heap(current_time)
                time.sleep(1)
            except Exception as e:
                log_error("Error in rarest piece update thread", e)
                time.sleep(5)
                
    def update_rarest_piece_min_heap(self, current_time):
        self._last_piece_update = current_time
        print("update")
        self.rarest_piece_min_heap = self.getRarestPieceMinHeap()
        
    def getRarestPieceMinHeap(self):
        piece_listOfpeers = {}
        for i in range(self.piece_manager.number_of_pieces):
            piece_listOfpeers[i] = []
        with timed_lock(self._connected_peers_lock.read_access, "_connected_peers_lock.read_access"):
            for peer in self._connected_peers:
                if not peer.connected:
                    print("disconnected peer in connected")
                if peer.peer_choking != 0:
                    continue
                if peer.bit_field:
                    for index in range(len(peer.bit_field)):
                        if peer.bit_field[index] and self.piece_manager.is_piece_empty(index):
                            piece_listOfpeers[index].append(peer)
        piece_listOfpeers = (sorted(piece_listOfpeers.items(), key =  #Сортировка кусков по числу пиров, у которых они есть
             lambda kv:(len(kv[1]), kv[0])))  
        result: list[tuple[int, list[Peer]]] = [] #Перемешивание в группах одинаковой редкости
        for _, group in groupby(piece_listOfpeers, key=lambda kv: len(kv[1])):
            group_list = list(group)
            random.shuffle(group_list)
            result.extend(group_list)
        # for _, peers in result: # Сортировка списка пиров по каждому куску по их «оценке»
            # peers.sort(key=lambda peer: peer.peer_score(), reverse=True) 
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
        log_info("Updating peer list from tracker...")
        try:
            self._last_peer_update = current_time
            with timed_lock(self._tracker_obj.peers_lock, "_tracker_obj.peers_lock"):
                peers_copy = self._tracker_obj.peers.copy()
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
            with timed_lock(self._connected_peers_lock.read_access, "_connected_peers_lock.read_access"):
                with timed_lock(self._peers_lock.read_access, "_peers_lock.read_access"):
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
        with timed_lock(self._connected_peers_lock.read_access, "_connected_peers_lock.read_access"):
            for peer in self._connected_peers:
                try:
                    peer.close()
                except:
                    pass
        self._clear_connected_peers()
        self._clear_peers()
        self._clear_peers_ip_port()

    def read_continously_from_sock(self, peer: Peer):
        if self._get_connected_peers_count() >= MAX_CONNECTED_PEER:
            return
        try:
            while True:
                msg = peer.getMessage()
                if msg:
                    try:
                        # if not peer.sock or not peer.connected or peer.check_inactivity():

                        message_length = msg[:4]
                        
                        if not message_length:
                            log_error(f"Invalid message length from {peer.ip_port}: {len(message_length)}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                            continue
                            
                        message_length = struct.unpack(">I", message_length)[0]
                        # log_info(f"{peer.ip_port}: got message with {message_length} bytes", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                        if message_length == 0:
                            continue
                            
                        message_ID = msg[4:5]
                        if not message_ID:
                            log_error(f"Invalid message ID from {peer.ip_port}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                            continue
                            
                        message_ID_u = struct.unpack(">B", message_ID)[0]
                        
                        if message_ID_u == 0:  # Choke
                            peer.peer_choking = 1
                            log_info(f"Peer {peer.ip_port} choked us", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                        elif message_ID_u == 1:  # Unchoke
                            peer.peer_choking = 0
                            log_info(f"Peer {peer.ip_port} unchoked us", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                        elif message_ID_u == 2:  # Interested
                            peer.peer_interested = 1
                            log_info(f"Peer {peer.ip_port} is interested", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                        elif message_ID_u == 3:  # Not interested
                            peer.peer_interested = 0
                            log_info(f"Peer {peer.ip_port} is not interested", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                        elif message_ID_u == 4:  # Have
                            if message_length - 1 == 4:
                                piece_index = msg[5:9]
                                if piece_index:
                                    piece_index = struct.unpack(">I", piece_index)[0]
                                else:
                                    log_error(f"{peer.ip_port} 'have' message: couldn't read piece_index", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                    continue
                                if piece_index < len(peer.bit_field):
                                    peer.bit_field[piece_index] = 1
                                    peer.got_bit_field = True
                                    log_info(f"{peer.ip_port} 'have' message received for piece {piece_index}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                else:
                                    log_error(f"{peer.ip_port} 'have' message: wrong piece_index ({piece_index}, more or eq than {len(peer.bit_field)})", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                            else:
                                log_error(f"{peer.ip_port} 'have' message: wrong length ({message_length}, not 5)", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                    
                        elif message_ID_u == 5:  # Bitfield
                            bitfield_length = message_length - 1
                            
                            bitfield_data = msg[5:5 + bitfield_length]
                            if bitfield_data:
                                if len(bitfield_data) == bitfield_length:
                                    peer.bit_field = bitstring.BitArray(bitfield_data)
                                    peer.got_bit_field = True
                                    log_info(f"Received valid bitfield from {peer.ip_port}: length is {len(bitfield_data)}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                else:
                                    log_error(f"Received invalid bitfield from {peer.ip_port}: length is {len(bitfield_data)}, need {bitfield_length}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                            else:
                                log_error(f"bitfield: couldn't read bitfield_data", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                
                        elif message_ID_u == 6:  # Request
                            # log_info(f"Received request from {peer.ip_port}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                            data = msg[5:17]
                            if data:
                                pass
                            else:
                                log_error(f"Request: couldn't read request data", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                
                            if len(data) < 12:
                                continue
                        elif message_ID_u == 7:  # Piece
                            try:
                                piece_index_b = msg[5:9]
                                if not piece_index_b:
                                    log_error(f"Invalid piece_index_b from {peer.ip_port}: {piece_index_b}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                    continue
                                block_offset_b = msg[9:13]
                                if not block_offset_b:
                                    log_error(f"Invalid piece_index_b from {peer.ip_port}: {block_offset_b}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                    continue
                                piece_index = struct.unpack(">I", piece_index_b)[0]
                                block_offset = struct.unpack(">I", block_offset_b)[0]
                                peer.pending_requests -= 1
                                # Validate piece index
                                # if piece_index >= self.piece_manager.number_of_pieces:
                                #     log_error(f"Invalid piece index {piece_index} from {peer.ip_port}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                #     continue
                                    
                                block_index = block_offset // BLOCK_SIZE
                                
                                # # Get piece safely
                                # if not self.piece_manager.is_index_valid(piece_index):
                                #     log_error(f"Invalid piece index {piece_index} from {peer.ip_port}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                #     continue
                                
                                # # Validate block index
                                # if block_index >= self.piece_manager.get_blocks_len(piece_index):
                                #     log_error(f"Invalid block index {block_index} for piece {piece_index} from {peer.ip_port}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                #     continue
                                
                                block_data = msg[13:message_length + 4]
                                if block_data is None:
                                    log_error(f"Invalid piece {piece_index} from {peer.ip_port}: couldn't read data", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                    continue
                                    
                                # Update block status safely
                                # log_info(f"{peer.ip_port}: update {piece_index}:{block_index}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')

                                # self.piece_manager.update_block_status_safe(piece_index, block_index, Status.DOWNLOADED, block_data)
                                if self.piece_manager.update_block_status_safe(piece_index, block_index, Status.DOWNLOADED, block_data):
                                # if Logger.measure_time(self.piece_manager.update_block_status_safe, "_10", piece_index, block_index, Status.DOWNLOADED, block_data):
                                # self.piece_manager.update_block_status_safe(piece_index, block_index, Status.DOWNLOADED, block_data)
                                    peer.blocks_recieved += 1
                                    self.piece_manager.downloaded_blocks += 1
                                # else:
                                    # log_error(f"Error in update_block_status_safe for {piece_index}:{block_index} from {peer.ip_port}: couldn't read data", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                    # Request next block if piece is not complete
                                    # if self.piece_manager.is_piece_empty(piece_index):
                                    # if Logger.measure_time(self.piece_manager.is_piece_empty, "_11", piece_index):
                                # self.prefetch_next_blocks(piece_index, peer)
                                        # Logger.measure_time(self.prefetch_next_blocks, "_12", piece_index, peer)
                                
                                # print(f"read sock wrote {piece_index}")
                                    
                            except struct.error as e:
                                log_error(f"Error unpacking piece message from {peer.ip_port}", e, flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                continue
                            except Exception as e:
                                log_error(f"Error processing piece message from {peer.ip_port}", e)
                                continue
                        else:
                            log_error(f"Unsupported message from {peer.ip_port}: message length: {message_length}, id: {message_ID_u}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                            
                                        
                    except socket.timeout:
                        continue
                    except ConnectionResetError:
                        log_error(f"Connection reset by peer {peer.ip_port}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                        break
                    except Exception as e:
                        log_error(f"Error reading from {peer.ip_port}", e, flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                        break
                else:
                    log_error("empty")
                        
        except Exception as e:
            log_error(f"Fatal error in read thread for {peer.ip_port}", e, flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
        finally:
            if self._is_peer_connected(peer):
                self._remove_connected_peer(peer)
                    
            try:
                peer.sock.close()
            except:
                pass
        
    def launch_thread(self, peer: Peer):
        p = threading.Thread(target=self.MultiThreadedConnection, args=(peer,))
        p.daemon = True
        p.start()
    
    def MultiThreadedConnection(self, peer:Peer):
        # if peer.ip != '5.79.98.162':
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
                
            # Set socket timeout for handshake
            sock.settimeout(5)  # Increased from 15 to 20 seconds
            
            # Send handshake with retry mechanism
            handshake = Handshake(self._tracker_obj.torrent_obj.peer_id, self._tracker_obj.torrent_obj.info_hash)
            handshake_success = False
            
            handshake_bytes = handshake.getHandshakeBytes()
            max_retries = 3
            for attempt in range(max_retries):  # Try handshake up to 3 times
                try:
                    peer._send_data(handshake_bytes)
                    peer.handshake_sent = True
                    
                    # Read handshake response with retry
                    data = None
                    try:
                        data = peer._read_bytes_from_sock(68)
                        if not data:
                            raise Exception
                    except socket.timeout:
                        log_error(f"Handshake timeout for {peer.ip_port} after {attempt + 1} attempts")
                        time.sleep(1)
                        continue
                    except Exception as e:
                        log_error(f"Error receiving handshake from {peer.ip_port} after {attempt + 1} attempts", e)
                        time.sleep(1)
                        continue
                    
                    if len(data) == 68:
                        # Verify handshake response
                        if data[0] != 19 or data[1:20] != b'BitTorrent protocol':
                            raise(f"Invalid handshake protocol")
                            break
                            
                        # Verify info hash
                        received_info_hash = data[28:48]
                        if received_info_hash != self._tracker_obj.torrent_obj.info_hash:
                            raise(f"Info hash mismatch")
                            break
                            
                        peer.handshake_received = True
                        handshake_success = True
                        log_info(f"Handshake successful with {peer.ip_port}")
                        break
                        
                except socket.timeout:
                    log_error(f"Handshake timeout for {peer.ip_port}")
                    if attempt == 2:  # Last attempt
                        peer.bad_peer = True
                        return
                    time.sleep(1)
                    continue
                except Exception as e:
                    log_error(f"Handshake error for {peer.ip_port}", e)
                    if attempt == 2:  # Last attempt
                        peer.bad_peer = True
                        return
                    time.sleep(1)
                    continue
            
            if not handshake_success:
                peer.bad_peer = True
                return
                
            # Add peer to connected list with proper locking
            if not self._is_peer_in_peers(peer):
                self._add_peer(peer)
            if not self._is_peer_in_peers_ip_port((peer.ip, peer.port)):
                self._add_peer_ip_port((peer.ip, peer.port))
            if not self._is_peer_connected(peer):    
                self._add_connected_peer(peer)
            peer.sock = sock
            
            # Send interested message with retry
            interested_sent = False
            interested_message = interested().byteStringForInterested()
            for attempt in range(max_retries):
                try:
                    if peer._send_data(interested_message):
                        peer.am_interested = 1
                    interested_sent = True
                    break
                except Exception as e:
                    if attempt == 2:  # Last attempt
                        log_error(f"Error sending interested message to {peer.ip_port}", e)
                        peer.bad_peer = True
                        return
                    time.sleep(1)
            
            if not interested_sent:
                peer.bad_peer = True
                return
                
            # Start reading thread
            sock.settimeout(None)  # Remove timeout for continuous reading
            # t = threading.Thread(target=self.read_continously_from_sock, args=(peer,))
            # self._set_item_in_threads(peer, t)
            # t.start()
            
            log_info(f"Successfully connected to peer {peer.ip_port}")
            
        except Exception as e:
            log_error(f"Connection error for {peer.ip_port}", e)
            if self._is_peer_connected(peer):
                self._remove_connected_peer(peer)
        finally:
            peer.connecting = False
            if sock is not None and not peer.connected:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except:
                    pass
                try:
                    sock.close()
                except:
                    pass
        if peer.connected:
            peer.launch_socket_thread()
            threading.current_thread().name = "read"
            self.read_continously_from_sock(peer)
            # t = threading.Thread(target=self.read_continously_from_sock, args=(peer,))
            # self._set_item_in_threads(peer, t)
            # t.start()

    def request_blockByteString(self, piece_index, block_index, block_size):
        """Create request message according to BitTorrent protocol"""
        try:
            # piece_index = piece.piece_index
            # Calculate block offset in bytes
            block_offset = block_index * BLOCK_SIZE
            
            # Ensure block size is valid
            # if block_size <= 0 or block_size > BLOCK_SIZE:
            #     log_error(f"Invalid block size {block_size} for piece {piece_index}")
            #     return None
                
            return request(piece_index, block_offset, block_size).byteStringForRequest()
        except Exception as e:
            log_error(f"Error creating request message for piece {piece_index}, block {block_index}", e)
            return None

    def prefetch_next_blocks(self, piece_index, peer: Peer):
        """Request next blocks from the same piece following BitTorrent protocol"""
        for block_index in self.piece_manager.get_empty_blocks(piece_index):
            try:
                current_time = time.time()
                peer.pending_requests += 1
                peer.requests_sent += 1
                peer.last_request_time = current_time
                peer.send_data(request(piece_index, block_index * BLOCK_SIZE, self.piece_manager.get_block_size(piece_index, block_index)).byteStringForRequest())
                self.piece_manager.update_block_status_safe(piece_index, block_index, Status.REQUESTED, last_requested=current_time)                            
                  #         if self.is_peer_connected(peer):
                #             self._remove_connected_peer(peer)
                    
            except Exception as e:
                log_error(f"Error requesting next block: {piece_index}:{block_index}", e)
                raise
            
        return 1

    # def findRate(self):
    #     sum = 0
    #     n = 0
    #     for peer in self.connected_peers:
    #         if peer.rate:
    #             sum += peer.rate
    #             n += 1
    #     try:
    #         rate = sum // n
    #         return rate
    #     except:
    #         return 0

    def update_optimistic_unchoke(self):
        """Update optimistic unchoke every 30 seconds"""
        current_time = time.time()
        if current_time - self.last_optimistic_unchoke >= OPTIMISTIC_UNCHOKE_INTERVAL:
            # Find a random choked peer that we're interested in
            with timed_lock(self._connected_peers_lock.read_access, "_connected_peers_lock.read_access"):
                choked_peers = [p for p in self._connected_peers if p.peer_choking and p.am_interested]
            if choked_peers:
                # Unchoke the previous optimistic peer if it exists
                if self.optimistic_unchoke_peer and self._is_peer_connected(self.optimistic_unchoke_peer):
                    try:
                        unchoke_msg = unchoke()
                        self.optimistic_unchoke_peer.send_data(unchoke_msg.byteStringForUnchoke())
                        self.optimistic_unchoke_peer.am_choking = 0
                    except Exception as e:
                        log_error(f"Error sending optimistic unchoke to {self.optimistic_unchoke_peer.ip_port}", e)
                
                # Select new optimistic peer
                self.optimistic_unchoke_peer = random.choice(choked_peers)
                try:
                    unchoke_msg = unchoke()
                    self.optimistic_unchoke_peer.send_data(unchoke_msg.byteStringForUnchoke())
                    self.optimistic_unchoke_peer.am_choking = 0
                    log_info(f"Optimistic unchoke for {self.optimistic_unchoke_peer.ip_port}")
                except Exception as e:
                    log_error(f"Error sending optimistic unchoke to {self.optimistic_unchoke_peer.ip_port}", e)
            
            self.last_optimistic_unchoke = current_time

    def get_best_peer(self, peers, min_rate=10, max_pending_requests=150):
        try:
            if not peers:
                log_error(f"No peers in get_best_peer", flags=['best_peer'])
                return None
                
            now = time.time()
            candidates = []
            coef = 1.2
            i = 1
            max_i = 80
            
            # Защищаем доступ к peers
            while not candidates and i < max_i:  # Ограничиваем количество итераций
                for peer in peers:
                    try:
                        if (peer.connected and peer.peer_choking == 0 and peer.pending_requests < max_pending_requests): # (getattr(peer, 'rate', 0) or 0) >= min_rate and
                            last_request = peer.last_request_time
                            # candidates.append((now - last_request, peer))
                            candidates.append((peer.blocks_recieved, peer))

                    except Exception as e:
                        log_error(f"Error checking peer {peer.ip_port}: {e}", flags=['best_peer'])
                        continue
                        
                if not candidates:
                    max_pending_requests = int(max_pending_requests * (coef ** i))
                    i += 1

                if max_i == i and not candidates:
                    log_info(f"iterations limit reached", flags=['best_peer'])
                    
                    for peer in peers:
                        try:
                            if (peer.connected and peer.peer_choking == 0):
                                last_request = peer.last_request_time
                                candidates.append((peer.blocks_recieved, peer))
                            else:
                                log_info(f"{peer.ip_port} not suitable: {peer.connected}, {peer.peer_choking == 0}")

                        except Exception as e:
                            log_error(f"Error checking peer {peer.ip_port}: {e}", flags=['best_peer'])
                            continue
            
            if not candidates:
                log_error(f"No candidates in get_best_peer", flags=['best_peer'])
                return None
                
            # Sort by time difference only, using a key function to avoid comparing Peer objects
            candidates.sort(key=lambda x: x[0], reverse=True)  # по убыванию времени простоя
            win = candidates[0][1]
            return win
        except Exception as e:
            log_error(f"Error in get_best_peer: {e}", flags=['best_peer'])
            return None

    def get_rarest_piece_min_heap_copy(self):
        return self.rarest_piece_min_heap.copy()

    def get_connected_peers_for_stats(self):
        with timed_lock(self._connected_peers_lock.read_access, "_connected_peers_lock.read_access"):
            return self._connected_peers.copy()

    def get_peers_for_progress(self):
        with timed_lock(self._peers_lock.read_access, "_peers_lock.read_access"):
            return self._peers.copy()

    def is_peer_connected(self, peer):
        return self._is_peer_connected(peer)

    def print_lock_statistics(self):
        """Выводит статистику использования locks для этого объекта"""
        print_lock_stats()