from torrent import Torrent
from math import ceil
from BlockandPiece import Piece, BLOCK_SIZE, Status, Block
import random
import colorama
from colorama import Fore, Back, Style
import time
import os
import datetime
from datetime import datetime
from logger import timed_lock, lock_decorator, print_lock_stats, Logger
from peer import Peer
from rwlock import RWLock
import threading
import traceback
import queue
from pathlib import Path
from typing import Iterator

colorama.init(autoreset=True)

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
        self.number_of_pieces = ceil(torrent.total_length / torrent.piece_length)
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
        

    @property
    def torrent(self):
        with timed_lock(self._torrent_lock.read_access, "_torrent_lock.read_access"):
            return self._torrent
    @torrent.setter
    def torrent(self, value):
        with timed_lock(self._torrent_lock.write_access, "_torrent_lock.write_access"):
            self._torrent = value

    @property
    def downloaded_count(self):
        with timed_lock(self._downloaded_count_lock.read_access, "_downloaded_count_lock.read_access"):
            return self._downloaded_count
    @downloaded_count.setter
    def downloaded_count(self, value):
        with timed_lock(self._downloaded_count_lock.write_access, "_downloaded_count_lock.write_access"):
            self._downloaded_count = value
            
    @property
    def requested_count(self):
        with timed_lock(self._requested_count_lock.read_access, "_requested_count_lock.read_access"):
            return self._requested_count
    @requested_count.setter
    def requested_count(self, value):
        with timed_lock(self._requested_count_lock.write_access, "_requested_count_lock.write_access"):
            self._requested_count = value
            
    @property
    def empty_count(self):
        with timed_lock(self._empty_count_lock.read_access, "_empty_count_lock.read_access"):
            return self._empty_count
    @empty_count.setter
    def empty_count(self, value):
        with timed_lock(self._empty_count_lock.write_access, "_empty_count_lock.write_access"):
            self._empty_count = value

    def _generate_pieces(self):
        last_piece = self.number_of_pieces - 1
        for i in range(self.number_of_pieces):
            if i == last_piece:
                piece_length = self.torrent.total_length - (self.number_of_pieces - 1) * self.torrent.piece_length
            else:
                piece_length = self.torrent.piece_length
            piece = Piece(i, piece_length, self._pieces_SHA1[i], self._ready_queue)
            self._totalBlocks += piece.number_of_blocks
            self._pieces.append(piece)
    
    def _getSHA1(self):
        for i in range(self.number_of_pieces):
            start = i * 20
            end = start + 20
            self._pieces_SHA1.append(self.torrent.pieces[start : end])

    def _merge_blocks(self, blocks):
        res = b""
        for block in blocks:
            res += block
        return res
        
    def _load_files(self):
        files = {}
        piece_offset = 0
        piece_size_used = 0
        for f in self.torrent.files:
            current_size_file = f["length"]
            file_offset = 0

            while current_size_file > 0:
                id_piece = int(piece_offset / self.torrent.piece_length)
                piece_size = self.get_piece_size(id_piece) - piece_size_used

                if current_size_file - piece_size < 0:
                    file = {"length": current_size_file,
                            "idPiece": id_piece,
                            "fileOffset": file_offset,
                            "pieceOffset": piece_size_used,
                            "path": f["path"]
                            }
                    piece_offset += current_size_file
                    file_offset += current_size_file
                    piece_size_used += current_size_file
                    current_size_file = 0

                else:
                    current_size_file -= piece_size
                    file = {"length": piece_size,
                            "idPiece": id_piece,
                            "fileOffset": file_offset,
                            "pieceOffset": piece_size_used,
                            "path": f["path"]
                            }
                    piece_offset += piece_size
                    file_offset += piece_size
                    piece_size_used = 0
                
                files.setdefault(id_piece, []).append(file)

        return files
        
    
    def _print_progress_bar_internal(self, suffix, decimals, print_matrix, peers):
        """Internal method for printing progress bar without locks"""
        # Calculate download speed
        out = ""
        out += f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n'
        out += self._progress_bar_string_internal(decimals, suffix, peers) 
        
        if peers is not None:
            out += self._peer_stats_string_internal(peers)   
        
        matrix_data = self._get_pieces_matrix_safe()
        if print_matrix and matrix_data:
            out += self._matrix_string_internal(matrix_data)
        
        try:
            os.makedirs('logs', exist_ok=True)
            with open(os.path.join('logs', "status.log"), 'w', encoding='utf-8') as f:
                f.write(out)
        except Exception as e:
            log_error(f'error in status: {e}')

    def _progress_bar_string_internal(self, decimals, suffix, peers):
        """Internal method for progress bar string without locks"""
        total = self._totalBlocks
        current_time = time.time()
        time_diff = current_time - self._last_update_time
        if time_diff >= 0.5:
            # if peers:
                # blocks_done = peers[0].blocks_recieved
            # else:
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
            percent = f"{100 * max((self._last_blocks_done / total), (self.num_of_downloaded_pieces()/self.number_of_pieces)):.{decimals}f}"
        else:
            percent = "0.0"
        
        return f'{percent}% {suffix} {speed_str} ETA: {eta_str}\n'

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
                peer_id = peer.ip
                # available = peer.avaliable_pieces(needed_pieces)
                
                
                # active = available > 0
                # if active:
                    # active_peers += 1
                strings = []    
                # sign = '+' if active else '-'
                # strings.append(f"{sign:<2}")
                strings.append(f"{peer_id:<15}")
                # strings.append(f"Pieces: {available:<4}")
                strings.append(f"Blocks: {peer.blocks_recieved:<4}")
                strings.append(f"Connections: {peer.requests_sent:<3}")
                # strings.append(f"Pending: {peer.pending_requests:<3}")
                strings.append(f"Pending: {peer.requests_sent - peer.blocks_recieved:<3}")
                strings.append(f"Bad: {'+' if peer.bad_peer else '-':<5}")
                strings.append(f"Unchocked: {'+' if not peer.peer_choking else '-':<5}")
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

    def monitor_block_timeouts_safe(self, timeout=3):
        for piece_index, piece in enumerate(self._pieces):
            piece_status = piece.monitor_block_timeouts(timeout)
            self._fix_sets(piece_status, piece_index)

    def is_need_to_download(self, piece_index: int):
        return self.is_piece_empty(piece_index)
    
    def get_piece_size(self, piece_index: int):
        if piece_index == self.number_of_pieces - 1:
            res = self.torrent.total_length - (self.number_of_pieces - 1) * self.torrent.piece_length
        else:
            res = self.torrent.piece_length
            
        return res
    
    def get_blocks_len(self, piece_index: int):
        res = ceil(self.get_piece_size(piece_index) / BLOCK_SIZE)
        
        # real = self.get_piece_safe(piece_index).number_of_blocks
        # assert res == real, f"get_blocks_len: params: {piece_index}; res: {res}; real: {real}"
        return res
    
    def get_block_size(self, piece_index, block_index):
        number_of_blocks = self.get_blocks_len(piece_index)
        piece_size = self.get_piece_size(piece_index)
        if number_of_blocks > 1:
            if piece_size % BLOCK_SIZE > 0 and number_of_blocks - 1 == block_index:
                res = piece_size % BLOCK_SIZE
            else:
                res = BLOCK_SIZE
        else:
            res = piece_size
        
        # real = self.get_piece_safe(piece_index)._blocks[block_index].block_size
        # assert res == real, f"get_block_size: params: {piece_index}:{block_index}; res: {res}; real: {real}"
        return res

    def get_piece_safe(self, piece_index: int) -> Piece:
        if 0 <= piece_index < self.number_of_pieces:
            return self._pieces[piece_index]
        return None
    
    def get_empty_blocks(self, piece_index: int) -> Iterator[int]:
        return self.get_piece_safe(piece_index).get_empty_blocks()
    
    def is_index_valid(self, piece_index: int) -> bool:
        return 0 <= piece_index < self.number_of_pieces

    def num_of_downloaded_pieces(self) -> int:
        # with self._status_counts_lock:
            res = self._status_counts[Status.DOWNLOADED.value]
            # real = sum(1 for p in self._pieces if p.is_complete())
            # assert abs(res - real) <= 1, f"num_of_downloaded_pieces: {res} {real}"
            return res

    def num_of_requested_pieces(self) -> int:
        # with self._status_counts_lock:
            res = self._status_counts[Status.REQUESTED.value]
            # real = sum(1 for p in self._pieces if p.is_requested())
            # assert abs(res - real) <= 1, f"num_of_requested_pieces: {res} {real}"
            return res

    def num_of_empty_pieces(self) -> int:
        # with self._status_counts_lock:
            res = self._status_counts[Status.EMPTY.value]
            # real = sum(1 for p in self._pieces if p.is_empty())
            # assert abs(res - real) <= 1, f"num_of_empty_pieces: {res} {real}"
            return res

    def is_piece_complete(self, piece_index: int) -> bool:
        # with self._status_counts_lock:
            res = self._pieces_statuses[piece_index] == Status.DOWNLOADED 
            # real = self.get_piece_safe(piece_index).is_complete()
            # assert res == real, f"is_piece_complete: params: {piece_index}; res: {res}; real: {real}"
            return res

    def is_piece_requested(self, piece_index: int) -> bool:
        # with self._status_counts_lock:
            res = self._pieces_statuses[piece_index] == Status.REQUESTED 
            # real = self.get_piece_safe(piece_index).is_requested()
            # assert res == real, f"is_piece_requested: params: {piece_index}; res: {res}; real: {real}"
            return res

    def is_piece_empty(self, piece_index: int) -> bool:
        # with self._status_counts_lock:
            res = self._pieces_statuses[piece_index] == Status.EMPTY 
            # real = self.get_piece_safe(piece_index).is_empty()
            # assert res == real, f"is_piece_empty: params: {piece_index}; res: {res}; real: {real}"
            return res

    def is_block_empty(self, piece_index: int, block_index: int) -> bool:
        return self.get_piece_safe(piece_index).is_block_empty(block_index)

    def all_piece_complete_safe(self):
        return self.num_of_downloaded_pieces() == self.number_of_pieces

    def print_progress_bar_safe(self, suffix='Complete', decimals=1, print_matrix=False, peers=None):
        try:
            self._print_progress_bar_internal(suffix, decimals, print_matrix, peers)
        except Exception as e:
            log_error(f"print_progress_bar_safe: {e}" )

    def update_block_status_safe(self, piece_index, block_index, state: Status, data=None, last_requested=None):
        try:
            p, c = self.get_piece_safe(piece_index).set_block_state(block_index, state, data, last_requested)
            self._fix_nums(p, c, piece_index)
            return True
        except Exception as e:
            log_error(f"update_block_status_safe {e}, \nTraceback:\n{traceback.format_exc()}")
            return False
    
    def _fix_nums(self, prev: Status, cur: Status, piece_index):
        if prev == cur:
            return
        with self._status_counts_lock:
            self._status_counts[prev.value] -= 1
            self._status_counts[cur.value] += 1
            self._pieces_statuses[piece_index] = cur
    
    def write_into_file_safe(self):
        
        base = Path(self.torrent.total_path)
        open_files = {}  # Path -> file object

        # Подготовить директории
        dirs = {
            base.joinpath(*entry['path']).parent
            for lst in self._files.values()
            for entry in lst
        }
        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)

        i = 0
        while True:
            idx, blocks = self._ready_queue.get()
            data = b"".join(blocks)
            for entry in self._files.get(idx, []):
                rel_path = entry['path']
                full_path = base.joinpath(*rel_path)
                fd = open_files.get(full_path)
                if fd is None:
                    # открываем файл один раз
                    mode = 'r+b' if full_path.exists() else 'wb'
                    fd = full_path.open(mode)
                    open_files[full_path] = fd

                start = entry['pieceOffset']
                end = start + entry['length']
                chunk = data[start:end]
                fd.seek(entry['fileOffset'])
                fd.write(chunk)

                i += 1

            print(i)

        # По завершении (где-то в конце работы):
        for fd in open_files.values():
            fd.close()
            

        else:
            # Single-file режим: пишем куски подряд
            pass
            # file_path = base / self.torrent.name
            # file_path.parent.mkdir(parents=True, exist_ok=True)
            # try:
            #     with file_path.open('wb') as f:
            #         for idx in range(self.number_of_pieces):
            #             if not self.is_piece_complete(idx):
            #                 continue
            #             data = self._merge_blocks(idx)
            #             if data:
            #                 f.write(data)
            # except PermissionError as e:
            #     print(f"Permission denied: {file_path} — {e}")
            # except Exception as e:
            #     print(f"Error writing {file_path}: {e}")

            
    def print_lock_statistics(self):
        """Выводит статистику использования locks для этого объекта"""
        print_lock_stats()
        