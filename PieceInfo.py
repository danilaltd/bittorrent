from torrent import Torrent
from math import ceil
from BlockandPiece import Piece, BLOCK_SIZE, Status
import time
import os
import datetime
from datetime import datetime
from logger import print_lock_stats
from peer import Peer
from rwlock import RWLock
import threading
import traceback
import queue
from pathlib import Path
from typing import Iterator
import bitstring

DISABLE_FILE_WRITE = True

def RoundUp(x):
    return ((x + 7) & (-8))

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
        name = 'pieceInfo.log'
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
        name = 'pieceInfo.log'
        path = 'logs'
        
    with open(os.path.join(f'{path}', f"{name}"), 'a', encoding='utf-8') as f:
        f.write(res)

class PieceInfo:
    def __init__(self, torrent):
        self._torrent: Torrent = torrent
        self._torrent_lock = RWLock("_torrent_lock")
        self._total_length = torrent.total_length
        self._piece_length = torrent.piece_length
        self.number_of_pieces = ceil(self._total_length / self._piece_length)
        self._pieces: list[Piece] = []
        
        self._status_counts = [self.number_of_pieces, 0, 0]
        self._status_counts_lock = threading.Lock()
        
        self._pieces_statuses = [Status.EMPTY] * self.number_of_pieces
        
        self.downloaded_blocks= 0
        self._pieces_SHA1 = []
        self._totalBlocks = 0
        self._ready_queue: queue.Queue[tuple[int, list[bytes]]] = queue.Queue()
        self._getSHA1()
        self._generate_pieces()
        self._files = self._load_files()
        self._last_update_time = time.time()
        self._last_blocks_done = 0
        self._last_bytes_done = 0
        self._download_speed = 0
        self._speed_history = []
        
        self._my_bitfield = bitstring.BitArray(RoundUp(self.number_of_pieces))
        self.bit_field_ready = False
        self._bit_field_lock = RWLock("")
        self._bit_field_len = len(self._my_bitfield)
        
        self.running = True
        self._monitor_timeouts_thread = threading.Thread(target=self.monitor_block_timeouts_safe_enter)
        self._monitor_timeouts_thread.start()
        
        self.file_thread = threading.Thread(target=self.write_into_files_enter)
        self.file_thread.start()
        

    def _generate_pieces(self):
        last_piece = self.number_of_pieces - 1
        for i in range(self.number_of_pieces):
            if i == last_piece:
                piece_length = self._total_length - (self.number_of_pieces - 1) * self._piece_length
            else:
                piece_length = self._piece_length
            piece = Piece(i, piece_length, self._pieces_SHA1[i], self._ready_queue)
            self._totalBlocks += piece.number_of_blocks
            self._pieces.append(piece)
    
    def _getSHA1(self):
        for i in range(self.number_of_pieces):
            start = i * 20
            end = start + 20
            self._pieces_SHA1.append(self._torrent.pieces[start : end])

    def monitor_block_timeouts_safe_enter(self):
        self.monitor_block_timeouts_safe()
        
    def write_into_files_enter(self):
        self.write_into_files()
        
    def _print_progress_bar_internal(self, decimals, print_matrix, peers):
        """Internal method for printing progress bar without locks"""
        # Calculate download speed
        out = ""
        out += f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n'
        out += self._progress_bar_string_internal(decimals, peers) 
        
        if peers is not None:
            out += self._peer_stats_string_internal(peers)   
        
        # matrix_data = self._get_pieces_matrix_safe()
        # if print_matrix and matrix_data:
            # out += self._matrix_string_internal(matrix_data)
        
        try:
            os.makedirs('logs', exist_ok=True)
            with open(os.path.join('logs', "status.log"), 'w', encoding='utf-8') as f:
                f.write(out)
        except Exception as e:
            log_error(f'error in status: {e}')

    def _progress_bar_string_internal(self, decimals, peers):
        """Internal method for progress bar string without locks"""
        total = self._totalBlocks
        current_time = time.time()
        time_diff = current_time - self._last_update_time
        if time_diff >= 0.5:
            blocks_done = self.downloaded_blocks
            current_bytes = blocks_done * BLOCK_SIZE
            bytes_diff = current_bytes - self._last_bytes_done
            
            current_speed = bytes_diff / time_diff if time_diff > 0 else 0
            
            self._speed_history.append(current_speed)
            if len(self._speed_history) > 5:
                self._speed_history.pop(0)
            
            self._download_speed = sum(self._speed_history) / len(self._speed_history)
            
            self._last_update_time = current_time
            self._last_blocks_done = blocks_done
            self._last_bytes_done = current_bytes

        # Format speed
        if self._download_speed > 1024 * 1024:
            speed_str = f"{self._download_speed/1024/1024:.1f} MB/s"
        elif self._download_speed > 1024:
            speed_str = f"{self._download_speed/1024:.1f} KB/s"
        else:
            speed_str = f"{self._download_speed:.1f} B/s"

        # Calculate ETA
        if self._download_speed > 0:
            remaining_bytes = (total - self._last_blocks_done) * BLOCK_SIZE
            eta_seconds = remaining_bytes / self._download_speed
            if eta_seconds > 3600:
                eta_str = f"{eta_seconds/3600:.1f}h"
            elif eta_seconds > 60:
                eta_str = f"{eta_seconds/60:.1f}m"
            else:
                eta_str = f"{eta_seconds:.0f}s"
        else:
            eta_str = "∞"

        if total > 0:
            percent1 = f"{100 * (self._last_blocks_done / total):.{decimals}f}"
            percent2 = f"{100 * (self.num_of_downloaded_pieces()/self.number_of_pieces):.{decimals}f}"
        else:
            percent1 = "0.0"
            percent2 = "0.0"
        
        suffix = f"{self.num_of_downloaded_pieces()}/{self.num_of_requested_pieces()}/{self.num_of_empty_pieces()}/{self.number_of_pieces}"
        
        return f'{percent1} {percent2}% {suffix} {speed_str} ETA: {eta_str}\n'

    def _matrix_string_internal(self, matrix_data):
        """Internal method for matrix string without locks"""
        res = ''
        matrix_width = 50
        matrix = []
        current_row = []
        
        for status in matrix_data:
            current_row.append(status)
            if len(current_row) == matrix_width:
                matrix.append(''.join(current_row))
                current_row = []
        
        if current_row:
            matrix.append(''.join(current_row) + ' ' * (matrix_width - len(current_row)))
        
        res += f'\nWidth: {matrix_width}\n'
        res += '\nBlock States:\n'
        for row in matrix:
            res += f"{row}\n"
        return res
        
    def _peer_stats_string_internal(self, peers: list[Peer]):
        """Internal method for peer stats string without locks"""
        res = ''
        try:
            res += '\nPeers: %d' % len(peers)
            # needed_pieces_count = self.number_of_pieces - self.num_of_downloaded_pieces()
            # needed_pieces = self.empty_pieces.copy()
            
            sorted_peers = sorted(peers, key=lambda peer: (not peer.connected, -peer.blocks_recieved))
            
            active_peers = 0
            border = False
            table = ''
            if sorted_peers:
                table += "Connected:\n"
            for peer in sorted_peers:
                if not border and not peer.connected:
                    table += "Not conected:\n"
                    border = True
                peer_id = f"{peer.ip}:{peer.port}"
                # available = peer.avaliable_pieces(needed_pieces)
                
                if peer.connected:
                    current_time = time.time()
                    time_diff = current_time - peer._last_update_time
                    if time_diff >= 0.5 or not len(peer._speed_history):
                        # if peers:
                            # blocks_done = peers[0].blocks_recieved
                        # else:
                        blocks_done = peer.blocks_recieved
                        current_bytes = blocks_done * BLOCK_SIZE
                        bytes_diff = current_bytes - peer._last_bytes_done
                        
                        current_speed = bytes_diff / time_diff if time_diff > 0 else 0
                        
                        peer._speed_history.append(current_speed)
                        if len(peer._speed_history) > 5:
                            peer._speed_history.pop(0)
                    
                        peer._download_speed = sum(peer._speed_history) / len(peer._speed_history)
                        
                        peer._last_update_time = current_time
                        peer._last_blocks_done = blocks_done
                        peer._last_bytes_done = current_bytes
                    
                    speed = peer._download_speed
                    
                    if speed > 1024 * 1024:
                        speed_str = f"{speed/1024/1024:.1f} MB/s"
                    elif speed > 1024:
                        speed_str = f"{speed/1024:.1f} KB/s"
                    else:
                        speed_str = f"{speed:.1f} B/s"
                        
                else:
                    speed_str = "-"
                    
                
                
                # active = available > 0
                # if active:
                    # active_peers += 1
                strings = []    
                # sign = '+' if active else '-'
                # strings.append(f"{sign:<2}")
                strings.append(f"{peer_id:<25}")
                # strings.append(f"Pieces: {available:<4}")
                strings.append(f"Blocks: {peer.blocks_recieved:<6}")
                strings.append(f"Connections: {peer.requests_sent:<7}")
                strings.append(f"Pending: {peer.pending_requests:<7}")
                strings.append(f"Canceled: {peer.canceled_requests:<7}")
                strings.append(f"Bad: {'+' if peer.bad_peer else '-':<3}")
                strings.append(f"Unchocked: {'+' if not peer.peer_choking else '-':<3}")
                strings.append(f"Speed: {speed_str:<7}")
                table += ' '.join(strings) + '\n'
        except Exception as e:
            res += f'\n[Peer stats error: {e}]\n'
            res += f'Traceback:\n{traceback.format_exc()}'
            print(f'\n[Peer stats error: {e}]\nTraceback:\n{traceback.format_exc()}')

        res += f' (active: {active_peers})\n'
        res += table
            
        return res

    def _get_pieces_matrix_safe(self):
        matrix = []
        for i in range(self.number_of_pieces):
            if self.is_piece_complete(i):
                matrix.append('*')  # Downloaded
            elif self.is_piece_requested(i):
                matrix.append('-')  # Currently downloading
            else:
                matrix.append(' ')  # Not downloaded
        return matrix

    def monitor_block_timeouts_safe(self, check_interval=5, timeout=25):
        while self.running:
            for piece_index, piece in enumerate(self._pieces):
                self.set_blocks_empty(piece_index, piece.get_blocks_to_reset(timeout))
            time.sleep(check_interval)
    def set_blocks_empty(self, piece_index, bad_blocks, skip_notify: bool = False):    
        if bad_blocks:
            cur_time = time.time()
            print(f"reset: {piece_index}:{bad_blocks}")
            for i in bad_blocks:
                self.update_block_status_safe(piece_index, i, Status.EMPTY, last_requested=cur_time, skip_notify=skip_notify)
                
    def is_need_to_download(self, piece_index: int):
        return self.is_piece_empty(piece_index)
    
    def _get_piece_size(self, piece_index: int):
        if piece_index == self.number_of_pieces - 1:
            res = self._total_length - piece_index * self._piece_length
        else:
            res = self._piece_length
        return res
    
    def get_block_size(self, piece_index, block_index):
        piece_size = self._get_piece_size(piece_index)
        number_of_blocks = (piece_size + BLOCK_SIZE - 1) // BLOCK_SIZE
        if number_of_blocks > 1:
            if number_of_blocks - 1 == block_index and piece_size % BLOCK_SIZE > 0:
                res = piece_size % BLOCK_SIZE
            else:
                res = BLOCK_SIZE
        else:
            res = piece_size
        return res

    def get_empty_blocks(self, piece_index: int) -> Iterator[int]:
        return self._pieces[piece_index].get_empty_blocks()
    
    def is_index_valid(self, piece_index: int) -> bool:
        return 0 <= piece_index < self.number_of_pieces

    def num_of_downloaded_pieces(self) -> int:
        res = self._status_counts[Status.DOWNLOADED.value]
        return res

    def num_of_requested_pieces(self) -> int:
        res = self._status_counts[Status.REQUESTED.value]
        return res

    def num_of_empty_pieces(self) -> int:
        res = self._status_counts[Status.EMPTY.value]
        return res

    def is_piece_complete(self, piece_index: int) -> bool:
        res = self._pieces_statuses[piece_index] == Status.DOWNLOADED 
        return res

    def is_piece_requested(self, piece_index: int) -> bool:
        res = self._pieces_statuses[piece_index] == Status.REQUESTED 
        return res

    def is_piece_empty(self, piece_index: int) -> bool:
        res = self._pieces_statuses[piece_index] == Status.EMPTY 
        return res

    def all_piece_complete_safe(self):
        return self.num_of_downloaded_pieces() == self.number_of_pieces

    def print_progress_bar_safe(self, decimals=1, print_matrix=False, peers=None):
        try:
            self._print_progress_bar_internal(decimals, print_matrix, peers)
        except Exception as e:
            log_error(f"print_progress_bar_safe: {e}" )

    def update_block_status_safe(self, piece_index: int, block_index, state: Status, data=None, last_requested=None, requested_by: Peer = None, skip_notify: bool = False):
        try:
            if state == Status.DOWNLOADED:
                self.downloaded_blocks += 1

            prev, cur = self._pieces[piece_index].set_block_state(block_index, state, data, last_requested, requested_by, skip_notify)
            self._fix_nums(prev, cur, piece_index)
            return True
        except Exception as e:
            log_error(f"update_block_status_safe {e}, \nTraceback:\n{traceback.format_exc()}")
            return False
    
    def _fix_nums(self, prev: Status, cur: Status, piece_index):
        if prev == cur:
            return
        # log_info(f"Piece {piece_index}, piece {piece_index}: {prev} → {cur}")
        with self._status_counts_lock:
            self._status_counts[prev.value] -= 1
            self._status_counts[cur.value] += 1
            self._pieces_statuses[piece_index] = cur
    
    
    def _load_files(self):
        files_by_piece = {}
        piece_offset = 0
        piece_length = self._piece_length

        for file_info in self._torrent.files:
            remaining = file_info['length']
            file_offset = 0
            path = file_info['path']

            while remaining > 0:
                piece_index, offset_in_piece = divmod(piece_offset, piece_length)
                # space left in current piece
                space_left = piece_length - offset_in_piece
                chunk = min(remaining, space_left)


                block = {
                    'length': chunk,
                    'piece_index': piece_index,
                    'file_offset': file_offset,
                    'piece_offset': offset_in_piece,
                    'path': path
                }
                files_by_piece.setdefault(piece_index, []).append(block)

                # advance counters
                remaining -= chunk
                piece_offset += chunk
                file_offset += chunk

        return files_by_piece
    
    
    def is_bit_set_in_bit_field(self, piece_index) -> bool:
        if self._bit_field_len <= piece_index:
            return False
        with self._bit_field_lock.read_access:
            return self._my_bitfield[piece_index]
    
    def set_bit_in_bit_field(self, piece_index) -> bool:
        if not self.is_bit_set_in_bit_field(piece_index) and self._bit_field_len > piece_index:
            with self._bit_field_lock.write_access:
                self._my_bitfield[piece_index] = 1
            self.bit_field_ready = True
            return True
        return False
    
    def get_my_bit_field(self) -> bytes:
        with self._bit_field_lock.read_access:
            return self._my_bitfield.copy()
    
    def write_into_files(self):
        
        base = Path(self._torrent.total_path)
        open_files = {}  # Path -> file object

        # Подготовить директории
        dirs = {
            base.joinpath(*entry['path']).parent
            for lst in self._files.values()
            for entry in lst
        }
        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)

        while True:
            try:
                idx, blocks = self._ready_queue.get(timeout=5)
            except queue.Empty:
                if not self.running: 
                    break 
                else: 
                    continue
            data = b"".join(blocks)
            self.set_bit_in_bit_field(idx)
            if not DISABLE_FILE_WRITE:
                for entry in self._files.get(idx, []):
                    rel_path = entry['path']
                    full_path = base.joinpath(*rel_path)
                    fd = open_files.get(full_path)
                    if fd is None:
                        mode = 'r+b' if full_path.exists() else 'wb'
                        fd = full_path.open(mode)
                        open_files[full_path] = fd

                    start = entry['piece_offset']
                    end = start + entry['length']
                    chunk = data[start:end]
                    fd.seek(entry['file_offset'])
                    fd.write(chunk)
                    fd.flush()

            # print(i)
            
        for fd in open_files.values():
            fd.flush()
            os.fsync(fd.fileno())
            fd.close()

            
    def print_lock_statistics(self):
        """Выводит статистику использования locks для этого объекта"""
        print_lock_stats()
        