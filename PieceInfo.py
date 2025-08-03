from torrent import Torrent
from math import ceil
from BlockandPiece import Piece, BLOCK_SIZE, Status
import time
import os
import datetime
from datetime import datetime
from peer import Peer
import threading
import traceback
import queue
from typing import Iterator

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
    def __init__(self, torrent: Torrent, ready_queue, downloaded_pieces: set[int]):
        self._total_length = torrent.total_length
        self._piece_length = torrent.piece_length
        self.number_of_pieces = ceil(self._total_length / self._piece_length)
        self._pieces: list[Piece] = []
        
        self._status_counts = [self.number_of_pieces - len(downloaded_pieces), 0, len(downloaded_pieces)]
        self._status_counts_lock = threading.Lock()
        
        self._pieces_statuses = [Status.EMPTY] * self.number_of_pieces
        
        self._ready_queue: queue.Queue[tuple[int, list[bytes]]] = ready_queue
        self._generate_pieces(downloaded_pieces)
        
        self.running = True

    def _generate_pieces(self, downloaded_pieces: set[int]):
        last_piece = self.number_of_pieces - 1
        for i in range(self.number_of_pieces):
            if i == last_piece:
                piece_length = self._total_length - (self.number_of_pieces - 1) * self._piece_length
            else:
                piece_length = self._piece_length
            downloaded = i in downloaded_pieces
            piece = Piece(i, piece_length, self._ready_queue, downloaded)
            self._pieces.append(piece)
        
    def write_into_files_enter(self):
        self.write_into_files()

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

    def monitor_block_timeouts_safe(self, timeout: int):
        for piece_index, piece in enumerate(self._pieces):
            self.set_blocks_empty(piece_index, piece.get_blocks_to_reset(timeout))
    
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
    
    def _get_block_len(self, piece_index: int):
        return (self._get_piece_size(piece_index) + BLOCK_SIZE - 1) // BLOCK_SIZE
    
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

    def update_block_status_safe(self, piece_index: int, block_index, state: Status, data=None, last_requested=None, requested_by: Peer = None, skip_notify: bool = False):
        try:
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