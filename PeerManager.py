from math import pi
from BlockandPiece import BLOCK_SIZE, Status, Block
from peer import Peer
from Messages import Handshake
from tracker import Tracker
from peer import Peer
import threading
import socket
from Messages import *
from PieceInfo import PieceInfo
import errno
import socket
import time
import random
import struct
import bitstring
import os
from datetime import datetime
from logger import timed_lock, lock_decorator, print_lock_stats
from rwlock import RWLock

MAX_CONNECTED_PEER = 50

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
        self._tracker_obj_lock = RWLock("_tracker_obj_lock")
        self._peers: list[Peer] = []
        self._peers_lock = RWLock("_peers_lock")
        self._peers_ip_port: list[tuple[str, str]] = []
        self._peers_ip_port_lock = RWLock("_peers_ip_port_lock")
        self._connected_peers: list[Peer] = []
        self._connected_peers_lock = RWLock("_connected_peers_lock")
        self._threads = {}
        self._threads_lock = RWLock("_threads_lock")
        self._piece_manager = PieceInfo(tracker_obj.torrent_obj)
        self._piece_manager_lock = RWLock("_piece_manager_lock")
        self._torrent_completed = False
        self._torrent_completed_lock = RWLock("_torrent_completed_lock")
        self._number_of_pieces = self._piece_manager.number_of_pieces
        self._number_of_pieces_lock = RWLock("_number_of_pieces_lock")
        self._optimistic_unchoke_interval = 30
        self._optimistic_unchoke_interval_lock = RWLock("_optimistic_unchoke_interval_lock")
        self._last_optimistic_unchoke = time.time()
        self._last_optimistic_unchoke_lock = RWLock("_last_optimistic_unchoke_lock")
        self._optimistic_unchoke_peer: Peer = None
        self._optimistic_unchoke_peer_lock = RWLock("_optimistic_unchoke_peer_lock")
        self._last_peer_update = time.time()
        self._last_peer_update_lock = RWLock("_last_peer_update_lock")
        self._peer_update_interval = 30
        self._peer_update_interval_lock = RWLock("_peer_update_interval_lock")
        self._reconnect_interval = 10
        self._reconnect_interval_lock = RWLock("_reconnect_interval_lock")
        self._last_reconnect = time.time()
        self._last_reconnect_lock = RWLock("_last_reconnect_lock")
        self._running = True
        self._running_lock = RWLock("_running_lock")
        self._rarest_piece_min_heap = self._piece_manager.getRarestPieceMinHeap(self._connected_peers)
        self._rarest_piece_min_heap_lock = RWLock("_rarest_piece_min_heap_lock")
        self._rarest_piece_thread = threading.Thread(target=self._periodic_rarest_piece_update)
        self._rarest_piece_thread.daemon = True
        self._rarest_piece_thread.start()
        self._peer_update_thread = threading.Thread(target=self._periodic_peer_update)
        self._peer_update_thread.daemon = True
        self._download_started = False
        self._download_started_lock = RWLock("_download_started_lock")
        self._peer_update_thread.start()

    @property
    def tracker_obj(self):
        with timed_lock(self._tracker_obj_lock.read_access, "_tracker_obj_lock.read_access"):
            return self._tracker_obj
    @tracker_obj.setter
    def tracker_obj(self, value):
        with timed_lock(self._tracker_obj_lock.write_access, "_tracker_obj_lock.write_access"):
            self._tracker_obj = value

    @property
    def peers(self):
        with timed_lock(self._peers_lock.read_access, "_peers_lock.read_access"):
            return self._peers
    @peers.setter
    def peers(self, value):
        with timed_lock(self._peers_lock.write_access, "_peers_lock.write_access"):
            self._peers = value
            
    @property
    def peers_ip_port(self):
        with timed_lock(self._peers_ip_port_lock.read_access, "_peers_ip_port_lock.read_access"):
            return self._peers_ip_port
    @peers_ip_port.setter
    def peers_ip_port(self, value):
        with timed_lock(self._peers_ip_port_lock.write_access, "_peers_ip_port_lock.write_access"):
            self._peers_ip_port = value

    @property
    def connected_peers(self):
        with timed_lock(self._connected_peers_lock.read_access, "_connected_peers_lock.read_access"):
            return self._connected_peers
    @connected_peers.setter
    def connected_peers(self, value):
        with timed_lock(self._connected_peers_lock.write_access, "_connected_peers_lock.write_access"):
            self._connected_peers = value

    @property
    def threads(self):
        with timed_lock(self._threads_lock.read_access, "_threads_lock.read_access"):
            return self._threads
    @threads.setter
    def threads(self, value):
        with timed_lock(self._threads_lock.write_access, "_threads_lock.write_access"):
            self._threads = value

    @property
    def piece_manager(self):
        with timed_lock(self._piece_manager_lock.read_access, "_piece_manager_lock.read_access"):
            return self._piece_manager
    @piece_manager.setter
    def piece_manager(self, value):
        with timed_lock(self._piece_manager_lock.write_access, "_piece_manager_lock.write_access"):
            self._piece_manager = value

    @property
    def torrent_completed(self):
        with timed_lock(self._torrent_completed_lock.read_access, "_torrent_completed_lock.read_access"):
            return self._torrent_completed
    @torrent_completed.setter
    def torrent_completed(self, value):
        with timed_lock(self._torrent_completed_lock.write_access, "_torrent_completed_lock.write_access"):
            self._torrent_completed = value

    @property
    def number_of_pieces(self):
        with timed_lock(self._number_of_pieces_lock.read_access, "_number_of_pieces_lock.read_access"):
            return self._number_of_pieces
    @number_of_pieces.setter
    def number_of_pieces(self, value):
        with timed_lock(self._number_of_pieces_lock.write_access, "_number_of_pieces_lock.write_access"):
            self._number_of_pieces = value

    @property
    def optimistic_unchoke_interval(self):
        with timed_lock(self._optimistic_unchoke_interval_lock.read_access, "_optimistic_unchoke_interval_lock.read_access"):
            return self._optimistic_unchoke_interval
    @optimistic_unchoke_interval.setter
    def optimistic_unchoke_interval(self, value):
        with timed_lock(self._optimistic_unchoke_interval_lock.write_access, "_optimistic_unchoke_interval_lock.write_access"):
            self._optimistic_unchoke_interval = value

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
    def last_peer_update(self):
        with timed_lock(self._last_peer_update_lock.read_access, "_last_peer_update_lock.read_access"):
            return self._last_peer_update
    @last_peer_update.setter
    def last_peer_update(self, value):
        with timed_lock(self._last_peer_update_lock.write_access, "_last_peer_update_lock.write_access"):
            self._last_peer_update = value

    @property
    def peer_update_interval(self):
        with timed_lock(self._peer_update_interval_lock.read_access, "_peer_update_interval_lock.read_access"):
            return self._peer_update_interval
    @peer_update_interval.setter
    def peer_update_interval(self, value):
        with timed_lock(self._peer_update_interval_lock.write_access, "_peer_update_interval_lock.write_access"):
            self._peer_update_interval = value

    @property
    def reconnect_interval(self):
        with timed_lock(self._reconnect_interval_lock.read_access, "_reconnect_interval_lock.read_access"):
            return self._reconnect_interval
    @reconnect_interval.setter
    def reconnect_interval(self, value):
        with timed_lock(self._reconnect_interval_lock.write_access, "_reconnect_interval_lock.write_access"):
            self._reconnect_interval = value

    @property
    def last_reconnect(self):
        with timed_lock(self._last_reconnect_lock.read_access, "_last_reconnect_lock.read_access"):
            return self._last_reconnect
    @last_reconnect.setter
    def last_reconnect(self, value):
        with timed_lock(self._last_reconnect_lock.write_access, "_last_reconnect_lock.write_access"):
            self._last_reconnect = value

    @property
    def running(self):
        with timed_lock(self._running_lock.read_access, "_running_lock.read_access"):
            return self._running
    @running.setter
    def running(self, value):
        with timed_lock(self._running_lock.write_access, "_running_lock.write_access"):
            self._running = value

    @property
    def rarest_piece_min_heap(self):
        with timed_lock(self._rarest_piece_min_heap_lock.read_access, "_rarest_piece_min_heap_lock.read_access"):
            return self._rarest_piece_min_heap
    @rarest_piece_min_heap.setter
    def rarest_piece_min_heap(self, value):
        with timed_lock(self._rarest_piece_min_heap_lock.write_access, "_rarest_piece_min_heap_lock.write_access"):
            self._rarest_piece_min_heap = value

    @property
    def rarest_piece_thread(self):
        return self._rarest_piece_thread
    @property
    def peer_update_thread(self):
        return self._peer_update_thread
    @property
    def download_started(self):
        with timed_lock(self._download_started_lock.read_access, "_download_started_lock.read_access"):
            return self._download_started
    @download_started.setter
    def download_started(self, value):
        with timed_lock(self._download_started_lock.write_access, "_download_started_lock.write_access"):
            self._download_started = value

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
            
            
            
    def _clear_threads(self):
        with timed_lock(self._threads_lock.write_access, "_threads_lock.write_access"):
            self._threads.clear()
            
    def _is_item_in_threads(self, item):
        with timed_lock(self._threads_lock.read_access, "_threads_lock.read_access"):
            return item in self._threads
       
    def _set_item_in_threads(self, k, v):
        with timed_lock(self._threads_lock.write_access, "_threads_lock.write_access"):
            self._threads[k] = v    
            
    def _del_item_from_threads(self, item):
        with timed_lock(self._threads_lock.write_access, "_threads_lock.write_access"):
            del self._threads[item]
    
    def _add_connected_peer(self, peer):
        with timed_lock(self._connected_peers_lock.write_access, "_connected_peers_lock.write_access"):
            self._connected_peers.append(peer)
        self.update_rarest_piece_min_heap()

    def _remove_connected_peer(self, peer):
        with timed_lock(self._connected_peers_lock.write_access, "_connected_peers_lock.write_access"):
            self._connected_peers.remove(peer)
        self.update_rarest_piece_min_heap()

    def _clear_connected_peers(self):
        with timed_lock(self._connected_peers_lock.write_access, "_connected_peers_lock.write_access"):
            self._connected_peers.clear()
        self.update_rarest_piece_min_heap()

    def _get_connected_peers_count(self):
        with timed_lock(self._connected_peers_lock.read_access, "_connected_peers_lock.read_access"):
            return len(self._connected_peers)

    def _is_peer_connected(self, peer):
        with timed_lock(self._connected_peers_lock.read_access, "_connected_peers_lock.read_access"):
            return peer in self._connected_peers

    def update_rarest_piece_min_heap(self):
        self.rarest_piece_min_heap = self._piece_manager.getRarestPieceMinHeap(self.connected_peers)

    def _periodic_rarest_piece_update(self):
        while self.running:
            try:
                self.update_rarest_piece_min_heap()
                time.sleep(60)
            except Exception as e:
                log_error("Error in rarest piece update thread", e)
                time.sleep(30)

    def _periodic_peer_update(self):
        while self.running:
            try:
                current_time = time.time()
                update = current_time - self.last_peer_update >= self.peer_update_interval
                if not update and not self.download_started:
                    downloaded = self.piece_manager.num_of_downloaded_pieces()
                    requested = self.piece_manager.num_of_requested_pieces()
                    update = downloaded + requested < 15
                    self.download_started = downloaded + requested >= 15
                if update:
                    self._update_peers(current_time)
                if current_time - self.last_reconnect >= self.reconnect_interval:
                    self._reconnect_peers()
                    self.last_reconnect = current_time
                time.sleep(1)
            except Exception as e:
                log_error("Error in peer update thread", e)
                time.sleep(5)

    def _update_peers(self, current_time):
        log_info("Updating peer list from tracker...")
        try:
            self.last_peer_update = current_time
            with timed_lock(self._tracker_obj.peers_lock, "_tracker_obj.peers_lock"):
                peers_copy = self._tracker_obj.peers.copy()
            for peer_ip_port in peers_copy:
                if not self._is_peer_in_peers_ip_port(peer_ip_port):
                    self._add_peer_ip_port(peer_ip_port)
                    peerObj = Peer(peer_ip_port, self._number_of_pieces)
                    self._add_peer(peerObj)
                    self.launch_thread(peerObj)
        except Exception as e:
            log_error("Error updating peer list", e)
            self.peer_update_interval = min(60, self.peer_update_interval * 1.5)

    def _reconnect_peers(self):
        try:
            disconnected_peers = [p for p in self.peers if p not in self.connected_peers and p.connection_attempts < p.max_connection_attempts]
            if disconnected_peers:
                log_info(f"Attempting to reconnect to {len(disconnected_peers)} peers...")
                def peer_value(peer: Peer):
                    needed_pieces = sum(1 for i in range(self.piece_manager.number_of_pieces) if not self.piece_manager.is_piece_complete(i) and peer.bit_field[i])
                    return needed_pieces
                sorted_peers = sorted(disconnected_peers, key=peer_value, reverse=True)
                for peer in sorted_peers:
                    if self._get_connected_peers_count() >= MAX_CONNECTED_PEER:
                        break
                    try:
                        if (peer in self.threads and self.threads[peer].is_alive()) or peer.bad_peer or peer.connecting:
                            continue
                        self.launch_thread(peer)
                    except Exception as e:
                        log_error(f"Error reconnecting to peer {peer.ip_port}", e)
        except Exception as e:
            log_error("Error in peer reconnection", e)

    def exitPeerThreads(self):
        self.running = False
        if self.peer_update_thread.is_alive():
            self.peer_update_thread.join(timeout=5)
        for peer, thread in self.threads.copy().items():
            if thread.is_alive():
                thread.join(timeout=5)
        for peer in self.connected_peers:
            try:
                if peer.sock:
                    peer.sock.close()
            except:
                pass
            self._clear_connected_peers()
        self._clear_peers()
        self._clear_peers_ip_port()
        self._clear_threads()

    def read_continously_from_sock(self, peer: Peer):
        if self._get_connected_peers_count() >= MAX_CONNECTED_PEER:
            return
                
        try:
            while True:
                try:
                    if not peer.sock or not peer.connected or peer.check_inactivity():
                        log_error(f"Invalid socket or peer state for {peer.ip_port}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                        break

                    message_length = self._read_bytes_from_sock(4, peer)
                    
                    if not message_length:
                        log_error(f"Invalid message length from {peer.ip_port}: {len(message_length)}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                        continue
                        
                    message_length = struct.unpack(">I", message_length)[0]
                    log_info(f"{peer.ip_port}: got message with {message_length} bytes", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                    if message_length == 0:
                        # Keep-alive message
                        peer.update_activity()
                        continue
                        
                    message_ID = self._read_bytes_from_sock(1, peer)
                    if not message_ID:
                        log_error(f"Invalid message ID from {peer.ip_port}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                        continue
                        
                    message_ID_u = struct.unpack(">B", message_ID)[0]
                    peer.update_activity()
                    
                    # Handle message based on ID
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
                            piece_index = self._read_bytes_from_sock(4, peer)
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
                        # bitfield_data = sock.recv(bitfield_length)
                        bitfield_data = self._read_bytes_from_sock(bitfield_length, peer)
                        
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
                        log_info(f"Received request from {peer.ip_port}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                        data = self._read_bytes_from_sock(12, peer)
                        if data:
                            pass
                        else:
                            log_error(f"bitfield: couldn't read request data", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                            
                        if len(data) < 12:
                            continue
                    elif message_ID_u == 7:  # Piece
                        try:
                            piece_index_b = self._read_bytes_from_sock(4, peer)
                            if not piece_index_b:
                                log_error(f"Invalid piece_index_b from {peer.ip_port}: {piece_index_b}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                continue
                            block_offset_b = self._read_bytes_from_sock(4, peer)
                            if not block_offset_b:
                                log_error(f"Invalid piece_index_b from {peer.ip_port}: {block_offset_b}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                continue
                            piece_index = struct.unpack(">I", piece_index_b)[0]
                            block_offset = struct.unpack(">I", block_offset_b)[0]
                            peer.pending_requests = max(0, peer.pending_requests - 1)
                            # Validate piece index
                            if piece_index >= len(self.piece_manager.pieces):
                                log_error(f"Invalid piece index {piece_index} from {peer.ip_port}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                continue
                                
                            block_index = block_offset // BLOCK_SIZE
                            
                            # Get piece safely
                            piece = self.piece_manager.get_piece_safe(piece_index)
                            if not piece:
                                log_error(f"Invalid piece index {piece_index} from {peer.ip_port}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                continue
                            
                            # Validate block index
                            if block_index >= len(piece.blocks):
                                log_error(f"Invalid block index {block_index} for piece {piece_index} from {peer.ip_port}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                continue
                            
                            block_data = self._read_bytes_from_sock(message_length - 9, peer)
                            if block_data is None:
                                log_error(f"Invalid piece {piece_index} from {peer.ip_port}: couldn't read data", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                                continue
                                
                            # Update block status safely
                            # print(f"read sock got piece {piece_index}")
                            log_info(f"{peer.ip_port}: update {piece_index}:{block_index}", flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')

                            if self.piece_manager.update_block_status_safe(piece_index, block_index, Status.RECEIVED, block_data):
                            
                                peer.blocks_recieved += 1
                                                                                
                                # Request next block if piece is not complete
                                if self.piece_manager.is_piece_empty(piece_index):
                                    self._request_next_block(piece.piece_index, peer)
                            
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
                        
        except Exception as e:
            log_error(f"Fatal error in read thread for {peer.ip_port}", e, flags = ['Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
        finally:
            if self._is_peer_connected(peer):
                self._remove_connected_peer(peer)
            if self._is_item_in_threads(peer):
                self._del_item_from_threads(peer)
                    
            try:
                peer.sock.close()
            except:
                pass

    def _request_next_block(self, piece_index, peer: Peer):
        """Request next block from the same piece"""
        try:
            # piece_index = piece.piece_index
            # Validate piece and peer state
            if not peer or peer.peer_choking:
                return
                
            # Find next block to request
            next_block = None
                
            # Get piece safely to check blocks
            piece_safe = self.piece_manager.get_piece_safe(piece_index)
            if not piece_safe:
                return
                
            for i in range(len(piece_safe.blocks)):
                if piece_safe.blocks[i].status == Status.EMPTY and not piece_safe.blocks[i].last_requested:
                    next_block = (i, piece_safe.blocks[i])
                    break
                    
            if not next_block:
                return
                
            block_index, block = next_block
            
            # Send request
            request_block = self.request_blockByteString(piece_index, block_index, block.block_size)
            if request_block:
                peer.send_data(request_block)
                # Update last_requested safely
                self.piece_manager.update_block_status_safe(
                    piece_index, block_index, Status.REQUESTED, 
                    last_requested=time.time()
                )
                
        except Exception as e:
            log_error(f"Error requesting next block for piece {piece_index}", e)

    @staticmethod
    def _read_bytes_from_sock(length: int, peer: Peer):
        if length <= 0:
            log_error(f"Invalid piece data length {length} from {peer.ip_port}", flags=['piece Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
            return None
            
        data = b''
        required = length
        timeout = 10  # 1 seconds timeout for reading piece data
        start_time = time.time()
        
        while required > 0:
            try:
                if time.time() - start_time > timeout:
                    log_error(f"Timeout reading piece data from {peer.ip_port}", flags=['piece Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                    return None
                    
                start = time.time()
                buff = peer.sock.recv(min(required, 65536))  # Increased buffer size to 64KB
                end = time.time()
                
                if buff and len(buff) > 0:
                    peer.rate = len(buff) // 125
                    peer.rate = peer.rate // (end - start) if (end - start) > 0 else 0
                    data += buff
                    required = length - len(data)
                    
            except socket.error as e:
                err = e.args[0]
                if err != errno.EAGAIN and err != errno.EWOULDBLOCK:
                    log_error(f"Socket error reading piece data from {peer.ip_port}", e, flags=['piece Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                    return None
                time.sleep(0.1) 
                continue
            except Exception as e:
                log_error(f"Error reading piece data from {peer.ip_port}", e, flags=['piece Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
                return None
                
        if len(data) != length:
            log_error(f"Received incomplete piece data from {peer.ip_port}: got {len(data)} bytes, expected {length}", flags=['piece Reading'], name=f'{peer.ip_port}({threading.current_thread().name}).log')
            return None
            
        return data
        
    def launch_thread(self, peer: Peer):
        p = threading.Thread(target=self.MultiThreadedConnection, args=(peer,))
        p.daemon = True
        p.start()
    
    def MultiThreadedConnection(self, peer:Peer):
        if peer.connection_attempts >= peer.max_connection_attempts:
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
                return
                
            # Set socket timeout for handshake
            sock.settimeout(5)  # Increased from 15 to 20 seconds
            
            # Send handshake with retry mechanism
            handshake = Handshake(self.tracker_obj.torrent_obj.peer_id, self.tracker_obj.torrent_obj.info_hash)
            handshake_success = False
            
            handshake_bytes = handshake.getHandshakeBytes()
            max_retries = 3
            for attempt in range(max_retries):  # Try handshake up to 3 times
                try:
                    peer.send_data(handshake_bytes)
                    peer.handshake_sent = True
                    
                    # Read handshake response with retry
                    data = None
                    try:
                        data = self._read_bytes_from_sock(68, peer)
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
                            log_error(f"Invalid handshake protocol from {peer.ip_port}")
                            break
                            
                        # Verify info hash
                        received_info_hash = data[28:48]
                        if received_info_hash != self.tracker_obj.torrent_obj.info_hash:
                            log_error(f"Info hash mismatch from {peer.ip_port}")
                            break
                            
                        peer.handshake_received = True
                        handshake_success = True
                        log_info(f"Handshake successful with {peer.ip_port}")
                        break
                        
                except socket.timeout:
                    if attempt == 2:  # Last attempt
                        log_error(f"Handshake timeout for {peer.ip_port}")
                        return
                    time.sleep(1)
                    continue
                except Exception as e:
                    if attempt == 2:  # Last attempt
                        log_error(f"Handshake error for {peer.ip_port}", e)
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
                    if peer.send_data(interested_message):
                        peer.am_interested = 1
                        peer.last_transmission = time.time()
                        peer.update_activity()
                    interested_sent = True
                    break
                except Exception as e:
                    if attempt == 2:  # Last attempt
                        log_error(f"Error sending interested message to {peer.ip_port}", e)
                        return
                    time.sleep(1)
            
            if not interested_sent:
                peer.bad_peer = True
                return
                
            # Start reading thread
            sock.settimeout(None)  # Remove timeout for continuous reading
            t = threading.Thread(target=self.read_continously_from_sock, args=(peer,))
            self._set_item_in_threads(peer, t)
            t.start()
            
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

    def request_blockByteString(self, piece_index, block_index, block_size):
        """Create request message according to BitTorrent protocol"""
        try:
            # piece_index = piece.piece_index
            # Calculate block offset in bytes
            block_offset = block_index * BLOCK_SIZE
            
            # Ensure block size is valid
            if block_size <= 0 or block_size > BLOCK_SIZE:
                log_error(f"Invalid block size {block_size} for piece {piece_index}")
                return None
                
            request_obj = request(piece_index, block_offset, block_size)
            return request_obj.byteStringForRequest()
        except Exception as e:
            log_error(f"Error creating request message for piece {piece_index}, block {block_index}", e)
            return None

    def prefetch_next_blocks(self, sock, piece_index, current_block_index, peer):
        """Request next blocks from the same piece following BitTorrent protocol"""
        try:
            # Validate inputs
            piece_safe = self.piece_manager.get_piece_safe(piece_index)
            if not peer or peer.peer_choking:
                return
            if current_block_index < 0 or current_block_index >= len(piece_safe.blocks):
                log_error(f"Invalid current block index {current_block_index} for piece {piece_index}")
                return
                
            # Get piece safely
            # piece_safe = piece
            if not piece_safe:
                return
                
            # Calculate how many blocks we can request (protocol limit)
            max_requests = 5  # Standard BitTorrent limit
            available_requests = max_requests - len([b for b in piece_safe.blocks if b.last_requested])
            
            if available_requests <= 0:
                return
                
            # Request next blocks in order
            for i in range(1, min(available_requests + 1, len(piece_safe.blocks) - current_block_index)):
                next_block_index = current_block_index + i
                if next_block_index >= len(piece_safe.blocks):
                    break
                    
                next_block: Block = piece_safe.blocks[next_block_index]
                if next_block.status == Status.EMPTY and not next_block.last_requested:
                    # Validate block size
                    if next_block.block_size <= 0 or next_block.block_size > BLOCK_SIZE:
                        log_error(f"Invalid block size {next_block.block_size} for piece {piece_index}")
                        continue
                        
                    request_block = self.request_blockByteString(piece_index, next_block_index, next_block.block_size)
                    if not request_block:
                        continue
                        
                    peer.pending_requests += 1
                    peer.requests_sent += 1
                    peer.last_request_time = time.time()
                    
                    # Используем новый метод для безопасной отправки
                    if not peer.send_data(request_block):
                        peer.pending_requests = max(0, peer.pending_requests - 1)
                        raise Exception("Failed to send prefetch request")
                        
                    # Update last_requested safely
                    self.piece_manager.update_block_status_safe(
                        piece_index, next_block_index, Status.REQUESTED,
                        last_requested=time.time()
                    )
                    
        except Exception as e:
            log_error(f"Error requesting next blocks for piece {piece_index}", e)

    def findRate(self):
        sum = 0
        n = 0
        for peer in self.connected_peers:
            if peer.rate:
                sum += peer.rate
                n += 1
        try:
            rate = sum // n
            return rate
        except:
            return 0

    def update_optimistic_unchoke(self):
        """Update optimistic unchoke every 30 seconds"""
        current_time = time.time()
        if current_time - self.last_optimistic_unchoke >= self.optimistic_unchoke_interval:
            # Find a random choked peer that we're interested in
            choked_peers = [p for p in self.connected_peers if p.peer_choking and p.am_interested]
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
                                log_info(f"not suitable: {peer.connected}, {peer.peer_choking == 0}")

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
        return self.connected_peers.copy()

    def get_peers_for_progress(self):
        return self.peers.copy()

    def is_peer_connected(self, peer):
        return self._is_peer_connected(peer)

    def print_lock_statistics(self):
        """Выводит статистику использования locks для этого объекта"""
        print_lock_stats()