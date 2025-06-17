from math import pi
from BlockandPiece import BLOCK_SIZE, Block
from peer import Handshake, Peer
from tracker import Tracker
from peer import Peer
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

MAX_CONNECTED_PEER = 50

# Красивый вывод ошибок
def log_error(msg, exc=None):
    RED = '\033[91m'
    END = '\033[0m'
    if exc is not None:
        if isinstance(exc, ConnectionResetError) or ('10054' in str(exc)):
            print(f'{RED}[ERROR][Network] Пир разорвал соединение (обычно для BitTorrent): {msg} — {exc}{END}', file=sys.stderr)
        else:
            print(f'{RED}[ERROR] {msg}: {exc}{END}', file=sys.stderr)
    else:
        print(f'{RED}[ERROR] {msg}{END}', file=sys.stderr)

# Красивый вывод ошибок и событий
def log_info(msg):
    GREEN = '\033[92m'
    END = '\033[0m'
    print(f'{GREEN}[INFO] {msg}{END}')

class PeerManager:
    def __init__(self, tracker_obj):
        self.tracker_obj = tracker_obj
        self.peers = []
        self.connected_peers = []
        self.threads = {}
        self.piece_manager = PieceInfo(tracker_obj.torrent_obj)
        self.torrent_completed = False
        self.number_of_pieces = self.piece_manager.number_of_pieces
        self.optimistic_unchoke_interval = 30  # 30 seconds
        self.last_optimistic_unchoke = time.time()
        self.optimistic_unchoke_peer = None
        self.pre_selected_pieces = []
        self.last_peer_update = time.time()
        self.peer_update_interval = 30  # Update peer list every 30 seconds
        self.reconnect_interval = 10  # Try to reconnect every 10 seconds
        self.last_reconnect = time.time()
        self.running = True
        self.peer_update_thread = threading.Thread(target=self._periodic_peer_update)
        self.peer_update_thread.daemon = True
        self.peer_update_thread.start()

    def _periodic_peer_update(self):
        """Periodically update peer list and reconnect to peers"""
        while self.running:
            try:
                current_time = time.time()
                
                # Update peer list from tracker
                if current_time - self.last_peer_update >= self.peer_update_interval:
                    log_info("Updating peer list from tracker...")
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
            disconnected_peers = [p for p in self.peers if p not in self.connected_peers]
            
            if disconnected_peers:
                log_info(f"Attempting to reconnect to {len(disconnected_peers)} peers...")
                
                # Try to reconnect to each disconnected peer
                for peer in disconnected_peers:
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
    def read_continously_from_sock(self, sock, peer : Peer):  
        if len(self.connected_peers) < MAX_CONNECTED_PEER:
            try:
                while True:
                    try:
                        message_length = sock.recv(4)
                        if not message_length or len(message_length) < 4:
                            log_error(f"Invalid message length from {peer.ip_port}")
                            break
                            
                        message_length = struct.unpack(">I", message_length)[0]
                        
                        if message_length == 0:
                            # Keep-alive message
                            continue
                            
                        message_ID = sock.recv(1)
                        if not message_ID:
                            log_error(f"Invalid message ID from {peer.ip_port}")
                            break
                            
                        message_ID_u = struct.unpack(">B", message_ID)[0]
                        
                        # Handle different message types according to protocol
                        if message_ID_u == 0:  # Choke
                            peer.peer_choking = 1
                            log_info(f"Peer {peer.ip_port} choked us")
                        elif message_ID_u == 1:  # Unchoke
                            peer.peer_choking = 0
                            log_info(f"Peer {peer.ip_port} unchoked us")
                            try:
                                interested_message = interested()
                                sock.send(interested_message.byteStringForInterested())
                                peer.am_interested = 1
                                self.send_initial_requests(sock, peer)
                            except Exception as e:
                                log_error(f"Error sending interested/requests after unchoke for {peer.ip_port}", e)
                        elif message_ID_u == 2:  # Interested
                            peer.peer_interested = 1
                            log_info(f"Peer {peer.ip_port} is interested")
                        elif message_ID_u == 3:  # Not interested
                            peer.peer_interested = 0
                            log_info(f"Peer {peer.ip_port} is not interested")
                        elif message_ID_u == 4:  # Have
                            piece_i = sock.recv(4)
                            if not piece_i or len(piece_i) < 4:
                                log_error(f"Invalid piece index received from {peer.ip_port}")
                                break
                            piece_index = struct.unpack(">I", piece_i)[0]
                            if piece_index < len(peer.bit_field):
                                peer.bit_field[piece_index] = True
                                log_info(f"{peer.ip_port} have message received for piece {piece_index}")
                        elif message_ID_u == 5:  # Bitfield
                            bitfield_length = message_length - 1
                            bitfield_data = sock.recv(bitfield_length)
                            if not bitfield_data or len(bitfield_data) < bitfield_length:
                                log_error(f"Invalid bitfield data received from {peer.ip_port}")
                                break
                            try:
                                # Create bitfield and validate bits
                                peer.bit_field = bitstring.BitArray(bitfield_data)
                                # Ensure bitfield length matches number of pieces
                                if len(peer.bit_field) > self.number_of_pieces:
                                    peer.bit_field = peer.bit_field[:self.number_of_pieces]
                                elif len(peer.bit_field) < self.number_of_pieces:
                                    peer.bit_field = peer.bit_field + bitstring.BitArray(self.number_of_pieces - len(peer.bit_field))
                                    
                                log_info(f"Received valid bitfield from {peer.ip_port}")
                            except Exception as e:
                                log_error(f"Error processing bitfield from {peer.ip_port}", e)
                                break
                        elif message_ID_u == 6:  # Request
                            if len(sock.recv(12)) < 12:
                                break
                        elif message_ID_u == 7:  # Piece
                            try:
                                piece_index = struct.unpack(">I", sock.recv(4))[0]
                                block_offset = struct.unpack(">I", sock.recv(4))[0]
                                
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
                                piece.blocks[block_index].status = 1
                                
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
                                
                    except socket.error as e:
                        if isinstance(e, ConnectionResetError) or ('10054' in str(e)):
                            log_error(f"Connection reset by peer {peer.ip_port}", e)
                        else:
                            log_error(f"Socket error for peer {peer.ip_port}", e)
                        break
                    except Exception as e:
                        log_error(f"Error processing message from {peer.ip_port}", e)
                        break
                        
            except Exception as e:
                log_error(f"Error in socket read thread for peer {getattr(peer, 'ip_port', 'unknown')}", e)
            finally:
                if peer in self.connected_peers:
                    self.connected_peers.remove(peer)
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
                if piece.blocks[i].status == 0 and not piece.blocks[i].last_requested:
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
                
                # Update pre-selected pieces if this is the last block
                if block_index == len(piece.blocks) - 1:
                    self.pre_selected_pieces = self.piece_manager.pre_select_next_pieces(
                        self.connected_peers,
                        piece.piece_index
                    )
                
        except Exception as e:
            log_error(f"Error requesting next block for piece {piece.piece_index}", e)

    @staticmethod
    def _read_piece_data(sock, length, peer):
        if length <= 0:
            log_error(f"Invalid piece data length {length} from {getattr(peer, 'ip_port', 'unknown')}")
            return None
            
        data = b''
        required = length
        timeout = 30  # 30 seconds timeout for reading piece data
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
        try:
            sock = peer.connect_to_peer()
            if sock is None:
                return
                
            # Set socket timeout for handshake
            sock.settimeout(10)
            
            # Send handshake
            handshake = Handshake(self.tracker_obj.torrent_obj.peer_id, self.tracker_obj.torrent_obj.info_hash)
            try:
                sock.send(handshake.getHandshakeBytes())
                data = sock.recv(68)
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
                
            # Add peer to connected list
            self.peers.append(peer)
            self.connected_peers.append(peer)
            peer.sock = sock
            
            # Send interested message
            try:
                interested_message = interested()
                sock.send(interested_message.byteStringForInterested())
                peer.am_interested = 1
                peer.last_transmission = time.time()
            except Exception as e:
                log_error(f"Error sending interested message to {peer.ip_port}", e)
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
            try:
                sock.close()
            except:
                pass
            return

    def get_peer_having_piece(self, piece : Piece):
        index = piece.piece_index
        peers_having_piece = []
        for peers in self.connected_peers:
            if peers.bit_field[index] == True:
                peers_having_piece.append(peers)
        return peers_having_piece
    def request_blockByteString(self, piece, block_index, block):
        """Create request message according to BitTorrent protocol"""
        # Calculate block offset in bytes
        block_offset = block_index * BLOCK_SIZE
        
        # Ensure block size is valid
        if block.block_size <= 0 or block.block_size > BLOCK_SIZE:
            log_error(f"Invalid block size {block.block_size} for piece {piece.piece_index}")
            return None
            
        request_obj = request(piece.piece_index, block_offset, block.block_size)
        return request_obj.byteStringForRequest()

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
                if next_block.status == 0 and not next_block.last_requested:
                    # Validate block size
                    if next_block.block_size <= 0 or next_block.block_size > BLOCK_SIZE:
                        log_error(f"Invalid block size {next_block.block_size} for piece {piece.piece_index}")
                        continue
                        
                    request_block = self.request_blockByteString(piece, next_block_index, next_block)
                    sock.send(request_block)
                    next_block.last_requested = time.time()
                    
        except Exception as e:
            log_error(f"Error requesting next blocks for piece {piece.piece_index}", e)

    def send_initial_requests(self, sock, peer):
        """Send initial batch of requests when peer becomes unchoked"""
        try:
            # Get a piece that this peer has
            for piece in self.piece_manager.pieces:
                if not piece.is_complete() and peer.bit_field[piece.piece_index]:
                    # Send initial requests (protocol limit)
                    blocks_to_request = min(5, len(piece.blocks))  # Standard BitTorrent limit
                    for block_index in range(blocks_to_request):
                        if piece.blocks[block_index].status == 0:  # Only request blocks we don't have
                            request_block = self.request_blockByteString(piece, block_index, piece.blocks[block_index])
                            if request_block:
                                sock.send(request_block)
                                piece.blocks[block_index].last_requested = time.time()
                    
                    # Pre-select next pieces after sending initial requests
                    self.pre_selected_pieces = self.piece_manager.pre_select_next_pieces(
                        self.connected_peers, 
                        piece.piece_index
                    )
                    break  # Only request from one piece initially
        except Exception as e:
            log_error(f"Error sending initial requests for {peer.ip_port}", e)

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

    def get_best_peer(self, peers):
        """Select the best peer based on download rate and connection stability"""
        if not peers:
            return None
        
        # Sort peers by their download rate
        sorted_peers = sorted(peers, key=lambda p: p.rate if p.rate else 0, reverse=True)
        
        # Prefer unchoked peers
        unchoked_peers = [p for p in sorted_peers if not p.peer_choking]
        if unchoked_peers:
            return unchoked_peers[0]
        return sorted_peers[0]