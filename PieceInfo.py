from torrent import Torrent
from math import ceil
from BlockandPiece import Piece, BLOCK_SIZE, BlockStatus
import random
import colorama
from colorama import Fore, Back, Style
import time
import os
from itertools import groupby
import datetime
from datetime import datetime
from logger import timed_lock, lock_decorator, print_lock_stats
from peer import Peer
from rwlock import RWLock
import threading
colorama.init(autoreset=True)

def log_info(msg, flags = None):
    if flags is None:
        flags = []
    flags.insert(0, f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_entry = f'{msg}\n'
    res = ''
    for flag in flags:
        res += f"[{flag}]"
    res += ' '    
    res += log_entry    
    with open(os.path.join('logs', "PieceInfo.log"), 'a', encoding='utf-8') as f:
        f.write(res)

class PieceInfo:
    def __init__(self, torrent):
        self._torrent: Torrent = torrent
        self._torrent_lock = RWLock("_torrent_lock")
        self._number_of_pieces = ceil(torrent.total_length / torrent.piece_length)
        self._number_of_pieces_lock = RWLock("_number_of_pieces_lock")
        self._pieces: list[Piece] = []
        self._pieces_lock = RWLock("_pieces_lock")
        self._pieces_SHA1 = []
        self._pieces_SHA1_lock = RWLock("_pieces_SHA1_lock")
        self._getSHA1()
        self._generate_piece()
        self._totalBlocks = self._getTotalBlocks()
        self._totalBlocks_lock = RWLock("_totalBlocks_lock")
        self._files = self._load_files()
        self._files_lock = RWLock("_files_lock")
        self._last_update_time = time.time()
        self._last_update_time_lock = RWLock("_last_update_time_lock")
        self._last_blocks_done = 0
        self._last_blocks_done_lock = RWLock("_last_blocks_done_lock")
        self._last_bytes_done = 0
        self._last_bytes_done_lock = RWLock("_last_bytes_done_lock")
        self._download_speed = 0
        self._download_speed_lock = RWLock("_download_speed_lock")
        self._speed_history = []
        self._speed_history_lock = RWLock("_speed_history_lock")
        self.tmplock = threading.Lock()

    @property
    def torrent(self):
        with timed_lock(self._torrent_lock.read_access, "_torrent_lock.read_access"):
            return self._torrent
    @torrent.setter
    def torrent(self, value):
        with timed_lock(self._torrent_lock.write_access, "_torrent_lock.write_access"):
            self._torrent = value

    @property
    def number_of_pieces(self):
        with timed_lock(self._number_of_pieces_lock.read_access, "_number_of_pieces_lock.read_access"):
            return self._number_of_pieces
    @number_of_pieces.setter
    def number_of_pieces(self, value):
        with timed_lock(self._number_of_pieces_lock.write_access, "_number_of_pieces_lock.write_access"):
            self._number_of_pieces = value

    @property
    def pieces(self):
        with timed_lock(self._pieces_lock.read_access, "_pieces_lock.read_access"):
            return self._pieces
    @pieces.setter
    def pieces(self, value):
        with timed_lock(self._pieces_lock.write_access, "_pieces_lock.write_access"):
            self._pieces = value

    @property
    def pieces_SHA1(self):
        with timed_lock(self._pieces_SHA1_lock.read_access, "_pieces_SHA1_lock.read_access"):
            return self._pieces_SHA1
    @pieces_SHA1.setter
    def pieces_SHA1(self, value):
        with timed_lock(self._pieces_SHA1_lock.write_access, "_pieces_SHA1_lock.write_access"):
            self._pieces_SHA1 = value

    @property
    def totalBlocks(self):
        with timed_lock(self._totalBlocks_lock.read_access, "_totalBlocks_lock.read_access"):
            return self._totalBlocks
    @totalBlocks.setter
    def totalBlocks(self, value):
        with timed_lock(self._totalBlocks_lock.write_access, "_totalBlocks_lock.write_access"):
            self._totalBlocks = value

    @property
    def files(self):
        with timed_lock(self._files_lock.read_access, "_files_lock.read_access"):
            return self._files
    @files.setter
    def files(self, value):
        with timed_lock(self._files_lock.write_access, "_files_lock.write_access"):
            self._files = value

    @property
    def last_update_time(self):
        with timed_lock(self._last_update_time_lock.read_access, "_last_update_time_lock.read_access"):
            return self._last_update_time
    @last_update_time.setter
    def last_update_time(self, value):
        with timed_lock(self._last_update_time_lock.write_access, "_last_update_time_lock.write_access"):
            self._last_update_time = value

    @property
    def last_blocks_done(self):
        with timed_lock(self._last_blocks_done_lock.read_access, "_last_blocks_done_lock.read_access"):
            return self._last_blocks_done
    @last_blocks_done.setter
    def last_blocks_done(self, value):
        with timed_lock(self._last_blocks_done_lock.write_access, "_last_blocks_done_lock.write_access"):
            self._last_blocks_done = value

    @property
    def last_bytes_done(self):
        with timed_lock(self._last_bytes_done_lock.read_access, "_last_bytes_done_lock.read_access"):
            return self._last_bytes_done
    @last_bytes_done.setter
    def last_bytes_done(self, value):
        with timed_lock(self._last_bytes_done_lock.write_access, "_last_bytes_done_lock.write_access"):
            self._last_bytes_done = value

    @property
    def download_speed(self):
        with timed_lock(self._download_speed_lock.read_access, "_download_speed_lock.read_access"):
            return self._download_speed
    @download_speed.setter
    def download_speed(self, value):
        with timed_lock(self._download_speed_lock.write_access, "_download_speed_lock.write_access"):
            self._download_speed = value

    @property
    def speed_history(self):
        with timed_lock(self._speed_history_lock.read_access, "_speed_history_lock.read_access"):
            return self._speed_history
    @speed_history.setter
    def speed_history(self, value):
        with timed_lock(self._speed_history_lock.write_access, "_speed_history_lock.write_access"):
            self._speed_history = value

    def _generate_piece(self):
        last_piece = self.number_of_pieces - 1
        for i in range(self.number_of_pieces):
            if i == last_piece:
                piece_length = self.torrent.total_length - (self.number_of_pieces - 1) * self.torrent.piece_length
                self.pieces.append(Piece(i, piece_length, self.pieces_SHA1[i]))
            else:
                self.pieces.append(Piece(i, self.torrent.piece_length, self.pieces_SHA1[i]))
    
    def _getSHA1(self):
        for i in range(self.number_of_pieces):
            start = i * 20
            end = start + 20
            self.pieces_SHA1.append(self.torrent.pieces[start : end])

    def _merge_blocks(self, index):
        res = b""
        piece = self.get_piece_safe(index)
        if not piece:
            return None
        for block in piece.blocks:
            if block.status != BlockStatus.RECEIVED or not block.data:
                return None
            res += block.data
        return res
        
    def _load_files(self):
        files = []
        piece_offset = 0
        piece_size_used = 0

        for f in self.torrent.files:
            current_size_file = f["length"]
            file_offset = 0

            while current_size_file > 0:
                id_piece = int(piece_offset / self.torrent.piece_length)
                piece = self.get_piece_safe(id_piece)
                if not piece:
                    break
                piece_size = piece.piece_size - piece_size_used

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

                files.append(file)
        return files
    
    def _getTotalBlocks(self):
        total = 0
        for i in range(len(self.pieces)):
            piece = self.get_piece_safe(i)
            if piece:
                total += len(piece.blocks)
        return total
    
    def _print_progress_bar_internal(self, iteration, total, suffix, decimals, print_matrix, peers, pieces_summary, matrix_data):
        """Internal method for printing progress bar without locks"""
        # Calculate download speed
        out = ""
        out += f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n'
        out += self._progress_bar_string_internal(iteration, total, decimals, suffix, pieces_summary) 
        
        # if peers is not None:
        #     out += self._peer_stats_string_internal(peers, pieces_summary)   
        
        # if print_matrix and matrix_data:
        #     out += self._matrix_string_internal(matrix_data)
        
        try:
            os.makedirs('logs', exist_ok=True)
            with open(os.path.join('logs', "status.log"), 'w', encoding='utf-8') as f:
                f.write(out)
        except Exception as e:
            print(f'error in status: {e}')

    def _progress_bar_string_internal(self, iteration, total, decimals, suffix, pieces_summary):
        """Internal method for progress bar string without locks"""
        current_time = time.time()
        if not hasattr(self, 'last_update_time'):
            self.last_update_time = current_time
            self.last_blocks_done = 0
            self.last_bytes_done = 0
            self.download_speed = 0
            self.speed_history = []
        else:
            time_diff = current_time - self.last_update_time
            if time_diff >= 0.5:
                blocks_done = pieces_summary['completed_blocks']
                current_bytes = blocks_done * BLOCK_SIZE
                bytes_diff = current_bytes - self.last_bytes_done
                
                current_speed = bytes_diff / time_diff if time_diff > 0 else 0
                
                self.speed_history.append(current_speed)
                if len(self.speed_history) > 5:
                    self.speed_history.pop(0)
                
                self.download_speed = sum(self.speed_history) / len(self.speed_history)
                
                self.last_update_time = current_time
                self.last_blocks_done = blocks_done
                self.last_bytes_done = current_bytes

        # Format speed
        if self.download_speed > 1024 * 1024:
            speed_str = f"{self.download_speed/1024/1024:.1f} MB/s"
        elif self.download_speed > 1024:
            speed_str = f"{self.download_speed/1024:.1f} KB/s"
        else:
            speed_str = f"{self.download_speed:.1f} B/s"

        # Calculate ETA
        if self.download_speed > 0:
            remaining_bytes = (total - self.last_blocks_done) * BLOCK_SIZE
            eta_seconds = remaining_bytes / self.download_speed
            if eta_seconds > 3600:
                eta_str = f"{eta_seconds/3600:.1f}h"
            elif eta_seconds > 60:
                eta_str = f"{eta_seconds/60:.1f}m"
            else:
                eta_str = f"{eta_seconds:.0f}s"
        else:
            eta_str = "∞"

        if total > 0:
            percent = f"{100 * (self.last_blocks_done / total):.{decimals}f}"
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
        
    def _peer_stats_string_internal(self, peers, pieces_summary):
        """Internal method for peer stats string without locks"""
        res = ''
        try:
            res += '\nPeers: %d' % len(peers)
            needed_pieces_count = pieces_summary['total_pieces'] - pieces_summary['completed_pieces']
            
            sorted_peers = sorted(peers, key=lambda peer: (not peer.connected, -peer.blocks_recieved))
            
            def is_active(peer):
                if hasattr(peer, 'bit_field') and peer.bit_field:
                    # Упрощенная проверка активности
                    return peer.connected and peer.blocks_recieved > 0
                return False
                
            active_peers = sum(1 for peer in sorted_peers if is_active(peer))
            res += f' (active: {active_peers})\n'
            
            border = False
            if sorted_peers:
                res += "Connected:\n"
            for idx, peer in enumerate(sorted_peers):
                if not border and not peer.connected:
                    res += "Not conected:\n"
                    border = True
                peer_id = peer.ip
                available = 0
                if hasattr(peer, 'bit_field') and peer.bit_field:
                    # Упрощенный подсчет доступных pieces
                    available = sum(1 for i in range(min(len(peer.bit_field), pieces_summary['total_pieces'])) 
                                  if peer.bit_field[i] and i >= pieces_summary['completed_pieces'])
                
                active = is_active(peer)
                sign = '+' if active else '-'
                res += f'  {sign} {peer_id}: {available} pieces; Blocks recieved: {peer.blocks_recieved}; Connections: {peer.requests_sent}; Pending: {peer.pending_requests}\n'
        except Exception as e:
            res += f'\n[Peer stats error: {e}]\n'
            
        return res

    def _get_pieces_summary_safe(self):
        pieces = self.pieces.copy()
        total_pieces = len(pieces)
        completed_pieces = 0
        total_blocks = 0
        completed_blocks = 0
        requested_blocks = 0
        for i in range(total_pieces):
            piece = pieces[i]
            if piece.is_complete():
                completed_pieces += 1
            total_blocks += len(piece.blocks)
            for block in piece.blocks:
                if block.status == BlockStatus.RECEIVED:
                    completed_blocks += 1
                elif block.last_requested and time.time() - block.last_requested < 30:
                    requested_blocks += 1
        res =  {
            'total_pieces': total_pieces,
            'completed_pieces': completed_pieces,
            'total_blocks': total_blocks,
            'completed_blocks': completed_blocks,
            'requested_blocks': requested_blocks
        }
        return res

    def get_pieces_matrix_safe(self):
        pieces = self.pieces.copy()
        matrix = []
        for piece in pieces:
            if piece.is_complete():
                matrix.append('*')  # Downloaded
            elif piece.is_requested():
                matrix.append('-')  # Currently downloading
            else:
                matrix.append(' ')  # Not downloaded
        return matrix

    def monitor_block_timeouts_safe(self, timeout=3):
        pieces = self.pieces.copy()
        for piece in pieces:
            for block in piece.blocks:
                if block.status == BlockStatus.REQUESTED and block.last_requested:
                    if time.time() - block.last_requested > timeout:
                        block.status = BlockStatus.EMPTY
                        block.last_requested = None
    
    def getRarestPieceMinHeap(self, connectedPeers):
        piece_listOfpeers = {}
        for i in range(self.number_of_pieces):
            piece_listOfpeers[i] = []
        for peer in connectedPeers:
            if peer.bit_field:
                for index, piece in enumerate(peer.bit_field):
                    if peer.bit_field[index]:
                        piece_listOfpeers[index].append(peer)             
        piece_listOfpeers = (sorted(piece_listOfpeers.items(), key =
             lambda kv:(len(kv[1]), kv[0])))  
        result: list[tuple[int, list[Peer]]] = []
        for _, group in groupby(piece_listOfpeers, key=lambda kv: len(kv[1])):
            group_list = list(group)
            random.shuffle(group_list)
            result.extend(group_list)
        for _, peers in result:
            peers.sort(key=lambda peer: peer.peer_score(), reverse=True) 
        return result

    def filter(self, src: list[tuple[int, list[Peer]]], length) -> list[tuple[Piece, list[Peer]]]:
        res = []
        i = 0
        for piece_i, peers in src:
            if peers:
                piece = self.get_piece_safe(piece_i)
                if piece is None or piece.is_complete() or piece.is_requested():
                    continue
                res.append((piece, peers))
                i += 1
                if i >= length:
                    break
        return res
            
    def is_need_to_download(self, piece_index: int):
        piece = self.get_piece_safe(piece_index)
        return not (piece is None or piece.is_complete() or piece.is_requested())
    
    def get_blocks_len(self, piece_index: int):
        piece = self.get_piece_safe(piece_index)
        return len(piece.blocks)

    def get_piece_safe(self, piece_index: int) -> Piece:
        pieces = self.pieces.copy()
        if 0 <= piece_index < len(pieces):
            return pieces[piece_index]
        return None

    def download_blocks_safe(self):
        summary = self._get_pieces_summary_safe()
        return summary['completed_blocks'], summary['requested_blocks']

    def all_piece_complete_safe(self):
        summary = self._get_pieces_summary_safe()
        res = summary['completed_pieces'] == summary['total_pieces']
        return res

    def pieces_downloaded_safe(self):
        summary = self._get_pieces_summary_safe()
        return summary['completed_pieces']

    def print_progress_bar_safe(self, iteration, total, suffix='Complete', decimals=1, print_matrix=False, peers=None):
        pieces_copy = self._get_pieces_summary_safe().copy()
        # matrix_copy = self.get_pieces_matrix_safe()
        matrix_copy = None
        self._print_progress_bar_internal(iteration, total, suffix, decimals, print_matrix, peers, pieces_copy, matrix_copy)

    def update_block_status_safe(self, piece_index, block_index, status, data=None, last_requested=None):
        pieces = self.pieces.copy()
        if 0 <= piece_index < len(pieces):
            piece = pieces[piece_index]
            if 0 <= block_index < len(piece.blocks):
                block = piece.blocks[block_index]
                block.status = status
                if data is not None:
                    block.data = data
                if last_requested is not None:
                    block.last_requested = last_requested
                return True
        return False

    def get_piece_status_safe(self, piece_index):
        pieces = self.pieces.copy()
        if 0 <= piece_index < len(pieces):
            piece = pieces[piece_index]
            return {
                'is_complete': piece.is_complete(),
                'is_requested': piece.is_requested(),
                'blocks_count': len(piece.blocks),
                'completed_blocks': sum(1 for b in piece.blocks if b.status == BlockStatus.RECEIVED)
            }
        return None

    def get_total_blocks_safe(self):
        return self._getTotalBlocks()

    def write_into_file_safe(self):
        files_copy = self.files.copy()
        master_i = 0
        n = len(files_copy)
        if self.torrent.multipleFiles:
            while master_i < n:
                piece_index = files_copy[master_i]['idPiece']
                length = files_copy[master_i]['length']
                file_offset = files_copy[master_i]['fileOffset']
                piece_offset = files_copy[master_i]['pieceOffset']
                path = files_copy[master_i]['path']
                piece_status = self.get_piece_status_safe(piece_index)
                if not piece_status or not piece_status['is_complete']:
                    master_i += 1
                    continue
                try:
                    path_to_file = os.path.join(self.torrent.total_path, *path)
                    os.makedirs(os.path.dirname(path_to_file), exist_ok=True)
                    try:
                        f = open(path_to_file, 'r+b')
                    except IOError:
                        f = open(path_to_file, 'wb')
                    piece_data = self._merge_blocks(piece_index)
                    if piece_data is not None:
                        data_to_be_written = piece_data[piece_offset : piece_offset + length]
                        f.seek(file_offset)
                        f.write(data_to_be_written)
                        f.close()
                except PermissionError as e:
                    print(f"Permission denied when writing to file: {path_to_file}")
                    print(f"Error: {str(e)}")
                except Exception as e:
                    print(f"Error writing to file: {str(e)}")
                finally:
                    if 'f' in locals():
                        f.close()
                master_i += 1
        else:
            try:
                file_path = os.path.join(self.torrent.total_path, self.torrent.name)
                f = open(file_path, 'wb')
                n = self.number_of_pieces
                while master_i < n:
                    piece_status = self.get_piece_status_safe(master_i)
                    if not piece_status or not piece_status['is_complete']:
                        master_i += 1
                        continue
                    piece_data = self._merge_blocks(master_i)
                    if piece_data is not None:
                        f.write(piece_data)
                    master_i += 1
            except PermissionError as e:
                print(f"Permission denied when writing to file: {file_path}")
                print(f"Error: {str(e)}")
            except Exception as e:
                print(f"Error writing to file: {str(e)}")
            finally:
                if 'f' in locals():
                    f.close()
        
    def print_lock_statistics(self):
        """Выводит статистику использования locks для этого объекта"""
        print_lock_stats()
        