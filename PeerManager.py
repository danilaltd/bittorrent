from math import pi
from BlockandPiece import BLOCK_SIZE, Block, Piece, BlockStatus
from peer import Handshake, Peer
from tracker import Tracker
from peer import Peer
from peer_connection import PeerConnection
import threading
import socket
from Messages import *
from PieceInfo import PieceInfo
import errno
import socket
import time
import sys
import random
import struct
import bitstring
import os
from datetime import datetime

MAX_CONNECTED_PEER = 50

# def log_error(msg, exc=None):
#     RED = '\033[91m'
#     END = '\033[0m'
#     if exc is not None:
#         if isinstance(exc, ConnectionResetError) or ('10054' in str(exc)):
#             print(f'{RED}[ERROR][Network] Пир разорвал соединение (обычно для BitTorrent): {msg} — {exc}{END}', file=sys.stderr)
#         else:
#             print(f'{RED}[ERROR] {msg}: {exc}{END}', file=sys.stderr)
#     else:
#         print(f'{RED}[ERROR] {msg}{END}', file=sys.stderr)

def log_error(msg, exc=None):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f'[{timestamp}] {msg}\n'
    if exc is not None:
        if isinstance(exc, ConnectionResetError) or ('10054' in str(exc)):
            log_entry = f'[ERROR][Network] Пир разорвал соединение (обычно для BitTorrent): {msg} — {exc}'
        else:
            log_entry = f'[ERROR] {msg}: {exc}'
    else:
        log_entry = f'[ERROR] {msg}'
    with open(os.path.join('logs', "peerManager.log"), 'a', encoding='utf-8') as f:
        f.write(log_entry)

# def log_info(msg):
#     GREEN = '\033[92m'
#     END = '\033[0m'
#     print(f'{GREEN}[INFO] {msg}{END}')
    
def log_info(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = f'[{timestamp}] {msg}\n'
    with open(os.path.join('logs', "peerManager.log"), 'a', encoding='utf-8') as f:
        f.write(log_entry)

class PeerManager:
    def __init__(self, tracker_obj):
        self.tracker_obj = tracker_obj
        self.peers = []
        self._connected_peers = []
        self.threads = {}
        self.piece_manager = PieceInfo(tracker_obj.torrent_obj)
        self.torrent_completed = False
        self.number_of_pieces = self.piece_manager.number_of_pieces
        self.optimistic_unchoke_interval = 30  # 30 seconds
        self.last_optimistic_unchoke = time.time()
        self.optimistic_unchoke_peer = None
        self.last_peer_update = time.time()
        self.peer_update_interval = 30  # Update peer list every 30 seconds
        self.reconnect_interval = 10  # Try to reconnect every 10 seconds
        self.last_reconnect = time.time()
        self.running = True
        self.rarest_piece_min_heap = self.piece_manager.getRarestPieceMinHeap(self._connected_peers)
        self._rarest_piece_thread = threading.Thread(target=self._periodic_rarest_piece_update)
        self._rarest_piece_thread.daemon = True
        self._rarest_piece_thread.start()
        self.peer_update_thread = threading.Thread(target=self._periodic_peer_update)
        self.peer_update_thread.daemon = True
        self.peer_update_thread.start()

    @property
    def connected_peers(self):
        return self._connected_peers

    @connected_peers.setter
    def connected_peers(self, value):
        self._connected_peers = value
        self.update_rarest_piece_min_heap()

    def update_rarest_piece_min_heap(self):
        self.rarest_piece_min_heap = self.piece_manager.getRarestPieceMinHeap(self._connected_peers)

    def _periodic_rarest_piece_update(self):
        while self.running:
            try:
                self.update_rarest_piece_min_heap()
                time.sleep(10)
            except Exception as e:
                log_error("Error in rarest piece update thread", e)
                time.sleep(5)

    def _periodic_peer_update(self):
        """Periodically update peer list and reconnect to peers"""
        while self.running:
            try:
                current_time = time.time()
                
                # Update peer list from tracker
                if current_time - self.last_peer_update >= self.peer_update_interval:
                    log_info("Updating peer list from tracker...")
                    try:
                        self.tracker_obj.get_peer_list()
                        self.last_peer_update = current_time
                        
                        # Add new peers
                        for peer in self.tracker_obj.peers:
                            if peer not in self.peers:
                                peer_obj = Peer(peer, self.number_of_pieces)
                                self.peers.append(peer_obj)
                                p = threading.Thread(target=self.MultiThreadedConnection, args=(peer_obj,))
                                p.daemon = True
                                p.start()
                    except Exception as e:
                        log_error("Error updating peer list", e)
                        # Reduce update interval on error
                        self.peer_update_interval = min(60, self.peer_update_interval * 1.5)
                
                # Try to reconnect to disconnected peers
                if current_time - self.last_reconnect >= self.reconnect_interval:
                    self._reconnect_peers()
                    self.last_reconnect = current_time
                    
                time.sleep(1)  # Sleep to prevent high CPU usage
                
            except Exception as e:
                log_error("Error in peer update thread", e)
                time.sleep(5)  # Sleep longer on error

    def _reconnect_peers(self):
        """Try to reconnect to disconnected peers"""
        try:
            # Get list of peers that should be connected but aren't
            disconnected_peers = [p for p in self.peers if p not in self.connected_peers and p.connection_attempts < p.max_connection_attempts]
            
            if disconnected_peers:
                log_info(f"Attempting to reconnect to {len(disconnected_peers)} peers...")
                
                # Sort peers by their potential value (based on bitfield)
                def peer_value(peer):
                    if not hasattr(peer, 'bit_field'):
                        return 0
                    # Count pieces we need that peer has
                    needed_pieces = sum(1 for i, piece in enumerate(self.piece_manager.pieces) 
                                     if not piece.is_complete() and peer.bit_field[i])
                    return needed_pieces
                
                sorted_peers = sorted(disconnected_peers, key=peer_value, reverse=True)
                
                # Try to reconnect to each disconnected peer
                for peer in sorted_peers:
                    if len(self.connected_peers) >= MAX_CONNECTED_PEER:
                        break
                        
                    try:
                        # Check if peer thread is still running
                        if peer in self.threads and self.threads[peer].is_alive():
                            continue
                            
                        # Start new connection thread
                        p = threading.Thread(target=self.MultiThreadedConnection, args=(peer,))
                        p.daemon = True
                        p.start()
                        self.threads[peer] = p
                        
                    except Exception as e:
                        log_error(f"Error reconnecting to peer {peer.ip_port}", e)
                        
        except Exception as e:
            log_error("Error in peer reconnection", e)

    def exitPeerThreads(self):
        """Stop all peer-related threads"""
        self.running = False
        if self.peer_update_thread.is_alive():
            self.peer_update_thread.join(timeout=5)
            
        for peer, thread in self.threads.items():
            if thread.is_alive():
                thread.join(timeout=5)
                
        # Close all peer connections
        for peer in self.connected_peers:
            try:
                if peer.sock:
                    peer.sock.close()
            except:
                pass
                
        self.connected_peers.clear()
        self.peers.clear()
        self.threads.clear()

    def connect(self):
        threads = []
        for peer in self.tracker_obj.peers:
            peer_obj = Peer(peer, self.number_of_pieces)
            p = threading.Thread(target=self.MultiThreadedConnection, args=(peer_obj, )) 
            p.start()

    def read_continously_from_sock(self, sock, peer: Peer):
        if len(self.connected_peers) < MAX_CONNECTED_PEER:
            try:
                while True:
                    try:
                        if not sock or not peer.connected or peer.check_inactivity():
                            log_error(f"Invalid socket or peer state for {peer.ip_port}")
                            break

                        message_length = sock.recv(4)
                        if not message_length or len(message_length) < 4:
                            log_error(f"Invalid message length from {peer.ip_port}")
                            break
                            
                        message_length = struct.unpack(">I", message_length)[0]
                        
                        if message_length == 0:
                            # Keep-alive message
                            peer.update_activity()
                            continue
                            
                        message_ID = sock.recv(1)
                        if not message_ID:
                            log_error(f"Invalid message ID from {peer.ip_port}")
                            break
                            
                        message_ID_u = struct.unpack(">B", message_ID)[0]
                        peer.update_activity()
                        
                        # Handle message based on ID
                        if message_ID_u == 0:  # Choke
                            peer.peer_choking = 1
                        elif message_ID_u == 1:  # Unchoke
                            peer.peer_choking = 0
                            log_info(f"Peer {peer.ip_port} unchoked us")
                        elif message_ID_u == 2:  # Interested
                            peer.peer_interested = 1
                        elif message_ID_u == 3:  # Not interested
                            peer.peer_interested = 0
                        elif message_ID_u == 4:  # Have
                            if message_length - 1 == 4:
                                piece_index = struct.unpack(">I", sock.recv(4))[0]
                                if piece_index < len(peer.bit_field):
                                    peer.bit_field[piece_index] = 1
                                    log_info(f"{peer.ip_port} have message received for piece {piece_index}")
                        elif message_ID_u == 5:  # Bitfield
                            bitfield_length = message_length - 1
                            bitfield_data = sock.recv(bitfield_length)
                            if len(bitfield_data) == bitfield_length:
                                peer.bit_field = bitstring.BitArray(bitfield_data)
                                log_info(f"Received valid bitfield from {peer.ip_port}")
                        elif message_ID_u == 6:  # Request
                            if len(sock.recv(12)) < 12:
                                break
                        elif message_ID_u == 7:  # Piece
                            try:
                                piece_index = struct.unpack(">I", sock.recv(4))[0]
                                block_offset = struct.unpack(">I", sock.recv(4))[0]
                                peer.pending_requests = max(0, peer.pending_requests - 1)
                                
                                # Validate piece index
                                if piece_index >= len(self.piece_manager.pieces):
                                    log_error(f"Invalid piece index {piece_index} from {peer.ip_port}")
                                    continue
                                    
                                piece = self.piece_manager.pieces[piece_index]
                                block_index = block_offset // BLOCK_SIZE
                                
                                # Validate block index
                                if block_index >= len(piece.blocks):
                                    log_error(f"Invalid block index {block_index} for piece {piece_index} from {peer.ip_port}")
                                    continue
                                
                                block_data = self._read_piece_data(sock, message_length - 9, peer)
                                if block_data is None:
                                    continue
                                    
                                piece.blocks[block_index].data = block_data
                                piece.blocks[block_index].status = BlockStatus.RECEIVED
                                
                                peer.blocks_recieved += 1
                                                                            
                                # Request next block if piece is not complete
                                if not piece.is_complete():
                                    self._request_next_block(sock, piece, peer)
                                elif not hasattr(piece, 'completion_logged') and not self.torrent_completed:
                                    piece.completion_logged = True
                                    
                            except struct.error as e:
                                log_error(f"Error unpacking piece message from {peer.ip_port}", e)
                                continue
                            except Exception as e:
                                log_error(f"Error processing piece message from {peer.ip_port}", e)
                                continue
                                    
                    except socket.timeout:
                        continue
                    except ConnectionResetError:
                        log_error(f"Connection reset by peer {peer.ip_port}")
                        break
                    except Exception as e:
                        log_error(f"Error reading from {peer.ip_port}", e)
                        break
                        
            except Exception as e:
                log_error(f"Fatal error in read thread for {peer.ip_port}", e)
            finally:
                if peer in self.connected_peers:
                    self.connected_peers.remove(peer)
                if peer in self.threads:
                    del self.threads[peer]
                try:
                    sock.close()
                except:
                    pass

    def _request_next_block(self, sock, piece, peer):
        """Request next block from the same piece"""
        try:
            # Validate piece and peer state
            if not piece or not peer or peer.peer_choking:
                return
                
            # Find next block to request
            next_block = None
            for i in range(len(piece.blocks)):
                if piece.blocks[i].status == BlockStatus.EMPTY and not piece.blocks[i].last_requested:
                    next_block = (i, piece.blocks[i])
                    break
                    
            if not next_block:
                return
                
            block_index, block = next_block
            
            # Send request
            request_block = self.request_blockByteString(piece, block_index, block)
            if request_block:
                sock.send(request_block)
                block.last_requested = time.time()
                
        except Exception as e:
            log_error(f"Error requesting next block for piece {piece.piece_index}", e)

    @staticmethod
    def _read_piece_data(sock, length, peer):
        if length <= 0:
            log_error(f"Invalid piece data length {length} from {getattr(peer, 'ip_port', 'unknown')}")
            return None
            
        data = b''
        required = length
        timeout = 3  # 30 seconds timeout for reading piece data
        start_time = time.time()
        
        while required > 0:
            try:
                if time.time() - start_time > timeout:
                    log_error(f"Timeout reading piece data from {getattr(peer, 'ip_port', 'unknown')}")
                    return None
                    
                start = time.time()
                buff = sock.recv(min(required, 65536))  # Increased buffer size to 64KB
                end = time.time()
                
                if not buff:  # Connection closed by peer
                    log_error(f"Connection closed by peer {getattr(peer, 'ip_port', 'unknown')}")
                    return None
                    
                if len(buff) > 0:
                    peer.rate = len(buff) // 125
                    peer.rate = peer.rate // (end - start) if (end - start) > 0 else 0
                    data += buff
                    required = length - len(data)
                    
            except socket.error as e:
                err = e.args[0]
                if err != errno.EAGAIN and err != errno.EWOULDBLOCK:
                    log_error(f"Socket error reading piece data from {getattr(peer, 'ip_port', 'unknown')}", e)
                    return None
                time.sleep(0.1)  # Small delay before retrying
                continue
            except Exception as e:
                log_error(f"Error reading piece data from {getattr(peer, 'ip_port', 'unknown')}", e)
                return None
                
        if len(data) != length:
            log_error(f"Received incomplete piece data from {getattr(peer, 'ip_port', 'unknown')}: got {len(data)} bytes, expected {length}")
            return None
            
        return data
        
    def MultiThreadedConnection(self, peer:Peer):
        if peer.connection_attempts >= peer.max_connection_attempts:
            log_info(f"Max connection attempts reached for {peer.ip_port}")
            return

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
            
            for attempt in range(3):  # Try handshake up to 3 times
                try:
                    handshake_bytes = handshake.getHandshakeBytes()
                    sock.send(handshake_bytes)
                    peer.handshake_sent = True
                    
                    # Read handshake response with retry
                    data = None
                    for retry in range(3):
                        try:
                            data = sock.recv(68)
                            if len(data) == 68:
                                break
                            time.sleep(1 * (retry + 1))
                        except socket.timeout:
                            if retry == 2:  # Last retry
                                log_error(f"Handshake timeout for {peer.ip_port} after {retry + 1} attempts")
                                break
                            continue
                        except Exception as e:
                            log_error(f"Error receiving handshake from {peer.ip_port}", e)
                            break
                    
                    if data and len(data) == 68:
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
                    time.sleep(2 * (attempt + 1))
                    continue
                except Exception as e:
                    if attempt == 2:  # Last attempt
                        log_error(f"Handshake error for {peer.ip_port}", e)
                        return
                    time.sleep(2 * (attempt + 1))
                    continue
            
            if not handshake_success:
                return
                
            # Add peer to connected list
            if peer not in self.peers:
                self.peers.append(peer)
            if peer not in self.connected_peers:
                self.connected_peers.append(peer)
            peer.sock = sock
            
            # Send interested message with retry
            interested_sent = False
            for attempt in range(3):
                try:
                    interested_message = interested()
                    sock.send(interested_message.byteStringForInterested())
                    peer.am_interested = 1
                    peer.last_transmission = time.time()
                    peer.update_activity()
                    interested_sent = True
                    break
                except Exception as e:
                    if attempt == 2:  # Last attempt
                        log_error(f"Error sending interested message to {peer.ip_port}", e)
                        return
                    time.sleep(1 * (attempt + 1))
            
            if not interested_sent:
                return
                
            # Start reading thread
            sock.settimeout(None)  # Remove timeout for continuous reading
            t = threading.Thread(target=self.read_continously_from_sock, args=(sock, peer))
            self.threads[peer] = t
            t.start()
            
            log_info(f"Successfully connected to peer {peer.ip_port}")
            
        except Exception as e:
            log_error(f"Connection error for {peer.ip_port}", e)
            if peer in self.connected_peers:
                self.connected_peers.remove(peer)
        finally:
            if sock is not None and not peer.connected:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except:
                    pass
                try:
                    sock.close()
                except:
                    pass

    def get_peer_having_piece(self, piece : Piece):
        index = piece.piece_index
        peers_having_piece = []
        for peers in self.connected_peers:
            if peers.bit_field[index] == True:
                peers_having_piece.append(peers)
        return peers_having_piece
    def request_blockByteString(self, piece, block_index, block):
        """Create request message according to BitTorrent protocol"""
        try:
            # Calculate block offset in bytes
            block_offset = block_index * BLOCK_SIZE
            
            # Ensure block size is valid
            if block.block_size <= 0 or block.block_size > BLOCK_SIZE:
                log_error(f"Invalid block size {block.block_size} for piece {piece.piece_index}")
                return None
                
            request_obj = request(piece.piece_index, block_offset, block.block_size)
            return request_obj.byteStringForRequest()
        except Exception as e:
            log_error(f"Error creating request message for piece {piece.piece_index}, block {block_index}", e)
            return None

    def prefetch_next_blocks(self, sock, piece, current_block_index, peer):
        """Request next blocks from the same piece following BitTorrent protocol"""
        try:
            # Validate inputs
            if not piece or not peer or peer.peer_choking:
                return
                
            if current_block_index < 0 or current_block_index >= len(piece.blocks):
                log_error(f"Invalid current block index {current_block_index} for piece {piece.piece_index}")
                return
                
            # Calculate how many blocks we can request (protocol limit)
            max_requests = 5  # Standard BitTorrent limit
            available_requests = max_requests - len([b for b in piece.blocks if b.last_requested])
            
            if available_requests <= 0:
                return
                
            # Request next blocks in order
            for i in range(1, min(available_requests + 1, len(piece.blocks) - current_block_index)):
                next_block_index = current_block_index + i
                if next_block_index >= len(piece.blocks):
                    break
                    
                next_block = piece.blocks[next_block_index]
                if next_block.status == BlockStatus.EMPTY and not next_block.last_requested:
                    # Validate block size
                    if next_block.block_size <= 0 or next_block.block_size > BLOCK_SIZE:
                        log_error(f"Invalid block size {next_block.block_size} for piece {piece.piece_index}")
                        continue
                        
                    request_block = self.request_blockByteString(piece, next_block_index, next_block)
                    peer.pending_requests += 1
                    peer.requests_sent += 1
                    peer.last_request_time = time.time()
                    try:
                        sock.send(request_block)
                    except:
                        peer.pending_requests = max(0, peer.pending_requests - 1)
                        raise
                    next_block.last_requested = time.time()
                    
        except Exception as e:
            log_error(f"Error requesting next blocks for piece {piece.piece_index}", e)

    def send_initial_requests(self, sock, peer):
        """Send initial batch of requests when peer becomes unchoked"""
        try:
            # Get pieces that this peer has and we need
            needed_pieces = [piece for piece in self.piece_manager.pieces 
                           if not piece.is_complete() and peer.bit_field[piece.piece_index]]
            
            if not needed_pieces:
                return
                
            # Sort pieces by rarity (pieces that fewer peers have are prioritized)
            def piece_rarity(piece):
                peers_with_piece = sum(1 for p in self.connected_peers 
                                     if p.bit_field[piece.piece_index])
                return 1.0 / (peers_with_piece + 1)  # Add 1 to avoid division by zero
                
            sorted_pieces = sorted(needed_pieces, key=piece_rarity, reverse=True)
            
            # Send requests for the rarest pieces first
            for piece in sorted_pieces[:5]:  # Request from up to 5 different pieces
                blocks_to_request = min(5, len(piece.blocks))  # Standard BitTorrent limit
                for block_index in range(blocks_to_request):
                    if piece.blocks[block_index].status == BlockStatus.EMPTY:  # Only request blocks we don't have
                        request_block = self.request_blockByteString(piece, block_index, piece.blocks[block_index])
                        if request_block:
                            sock.send(request_block)
                            piece.blocks[block_index].last_requested = time.time()
            
        except Exception as e:
            log_error(f"Error sending initial requests for {peer.ip_port}", e)

    def handle_unchoke(self, sock, peer):
        """Handle unchoke message from peer"""
        try:
            if not sock or not peer.connected:
                log_error(f"Invalid socket or peer state for {peer.ip_port}")
                return
                
            peer.peer_choking = 0
            log_info(f"Peer {peer.ip_port} unchoked us")
            
            # Send interested message if not already interested
            if not peer.am_interested:
                interested_message = interested()
                sock.send(interested_message.byteStringForInterested())
                peer.am_interested = 1
                peer.last_transmission = time.time()
            
            # Send initial requests with optimized block selection
            self.send_initial_requests(sock, peer)
            
            # Update optimistic unchoke
            self.update_optimistic_unchoke()
            
        except Exception as e:
            log_error(f"Error handling unchoke for {peer.ip_port}", e)

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

    def showRatePeers(self):
        rates = []
        for peer in self.connected_peers:
            if peer.rate:
                rates.append((peer, peer.rate))
        return rates

    def update_optimistic_unchoke(self):
        """Update optimistic unchoke every 30 seconds"""
        current_time = time.time()
        if current_time - self.last_optimistic_unchoke >= self.optimistic_unchoke_interval:
            # Find a random choked peer that we're interested in
            choked_peers = [p for p in self.connected_peers if p.peer_choking and p.am_interested]
            if choked_peers:
                # Unchoke the previous optimistic peer if it exists
                if self.optimistic_unchoke_peer and self.optimistic_unchoke_peer in self.connected_peers:
                    try:
                        unchoke_msg = unchoke()
                        self.optimistic_unchoke_peer.sock.send(unchoke_msg.byteStringForUnchoke())
                        self.optimistic_unchoke_peer.am_choking = 0
                    except Exception as e:
                        log_error(f"Error sending optimistic unchoke to {self.optimistic_unchoke_peer.ip_port}", e)
                
                # Select new optimistic peer
                self.optimistic_unchoke_peer = random.choice(choked_peers)
                try:
                    unchoke_msg = unchoke()
                    self.optimistic_unchoke_peer.sock.send(unchoke_msg.byteStringForUnchoke())
                    self.optimistic_unchoke_peer.am_choking = 0
                    log_info(f"Optimistic unchoke for {self.optimistic_unchoke_peer.ip_port}")
                except Exception as e:
                    log_error(f"Error sending optimistic unchoke to {self.optimistic_unchoke_peer.ip_port}", e)
            
            self.last_optimistic_unchoke = current_time

    def get_best_peer(self, peers, min_rate=10, max_pending_requests=10):
        """
        Выбрать пира, которому дольше всего не отправляли запросы, но с учётом скорости и лимита запросов.
        - min_rate: минимальная скорость (кБ/с), чтобы пир считался кандидатом
        - max_pending_requests: максимум незавершённых запросов к одному пиру
        """
        now = time.time()
        candidates = []
        coef = 1.2
        i = 1
        while not candidates:
            for peer in peers:
                if (peer.connected and 
                    peer.peer_choking == 0 and
                    # (getattr(peer, 'rate', 0) or 0) >= min_rate and
                    peer.pending_requests < max_pending_requests):
                    last_request = peer.last_request_time
                    candidates.append((now - last_request, peer))
            max_pending_requests = int(max_pending_requests * (coef ** i))
            i += 1

        candidates.sort(reverse=True)  # по убыванию времени простоя
        win = candidates[0][1]
        # win.pending_requests = int(win.pending_requests // 1.4)
        return win

    def _handle_peer_connection(self, peer: Peer):
        connection = PeerConnection(peer, self.piece_manager, self.number_of_pieces)
        
        if not connection.connect():
            return

        try:
            # Add peer to connected list after successful connection
            if peer not in self.connected_peers:
                self.connected_peers.append(peer)
            
            # Send handshake
            handshake = Handshake(self.tracker_obj.torrent_obj.peer_id, self.tracker_obj.torrent_obj.info_hash)
            try:
                connection.send_message(handshake.getHandshakeBytes())
                data = connection.sock.recv(68)
                if len(data) != 68:
                    log_error(f"Invalid handshake response length from {peer.ip_port}: {len(data)}")
                    return
                    
                # Verify handshake response
                if data[0] != 19 or data[1:20] != b'BitTorrent protocol':
                    log_error(f"Invalid handshake protocol from {peer.ip_port}")
                    return
                    
                log_info(f"Handshake successful with {peer.ip_port}")
                
            except socket.timeout:
                log_error(f"Handshake timeout for {peer.ip_port}")
                return
            except Exception as e:
                log_error(f"Handshake error for {peer.ip_port}", e)
                return

            # Send interested message
            try:
                interested_message = interested()
                connection.send_message(interested_message.byteStringForInterested())
                peer.am_interested = 1
                peer.last_transmission = time.time()
            except Exception as e:
                log_error(f"Error sending interested message to {peer.ip_port}", e)
                return

            while connection.running:
                message = connection.read_message()
                if message is None:
                    break

                message_id, length = message
                
                if message_id == 0:  # Choke
                    connection.handle_choke()
                elif message_id == 1:  # Unchoke
                    connection.handle_unchoke()
                elif message_id == 2:  # Interested
                    connection.handle_interested()
                elif message_id == 3:  # Not interested
                    connection.handle_not_interested()
                # Add other message handlers as needed

        except Exception as e:
            log_error(f"Error handling peer connection for {peer.ip_port}", e)
        finally:
            connection.close()
            if peer in self.connected_peers:
                self.connected_peers.remove(peer)