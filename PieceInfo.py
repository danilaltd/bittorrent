from torrent import Torrent
from math import ceil
from BlockandPiece import Piece, BLOCK_SIZE, Status, Block
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
import traceback
import queue
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
        
        self._statuses = [Status.EMPTY] * self._number_of_pieces
        self._statuses_lock = RWLock("_statuses_lock")
        self._downloaded_count = 0
        self._downloaded_count_lock = RWLock("_downloaded_count_lock")
        self._requested_count = 0
        self._requested_count_lock = RWLock("_requested_count_lock")
        self._empty_count = self._number_of_pieces
        self._empty_count_lock = RWLock("_empty_count_lock")

        
        
        self._downloaded_blocks= 0
        self._downloaded_blocks_lock_int = RWLock("_downloaded_blocks_lock_int")
        self._pieces_SHA1 = []
        self._pieces_SHA1_lock = RWLock("_pieces_SHA1_lock")
        self._totalBlocks = 0
        self._totalBlocks_lock = RWLock("_totalBlocks_lock")
        self._getSHA1()
        self._generate_pieces()
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
        
        self._queue = queue.Queue()
        self._worker = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker.start()


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
    def statuses(self):
        with timed_lock(self._statuses_lock.read_access, "_statuses_lock.read_access"):
            return self._statuses
    @statuses.setter
    def statuses(self, value):
        with timed_lock(self._statuses_lock.write_access, "_statuses_lock.write_access"):
            self._statuses = value
            
    def _set_status(self, index: int, new_status: Status):
        with timed_lock(self._statuses_lock.write_access, "_statuses_lock.write_access"):
            old_status = self._statuses[index]
            if old_status is new_status:
                return  # ничего не менялось
            self._statuses[index] = new_status
            

        if old_status is Status.EMPTY:
            self.empty_count -= 1
        elif old_status is Status.REQUESTED:
            self.requested_count -= 1
        elif old_status is Status.RECEIVED:
            self.downloaded_count -= 1

        if new_status is Status.EMPTY:
            self.empty_count += 1
        elif new_status is Status.REQUESTED:
            self.requested_count += 1
        elif new_status is Status.RECEIVED:
            self.downloaded_count += 1
            
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
    
    @property
    def downloaded_blocks(self):
        with timed_lock(self._downloaded_blocks_lock_int.read_access, "_downloaded_blocks_lock_int.read_access"):
            return self._downloaded_blocks
    @downloaded_blocks.setter
    def downloaded_blocks(self, value):
        with timed_lock(self._downloaded_blocks_lock_int.write_access, "_downloaded_blocks_lock_int.write_access"):
            self._downloaded_blocks = value

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

    def _generate_pieces(self):
        last_piece = self.number_of_pieces - 1
        for i in range(self.number_of_pieces):
            if i == last_piece:
                piece_length = self.torrent.total_length - (self.number_of_pieces - 1) * self.torrent.piece_length
            else:
                piece_length = self.torrent.piece_length
            piece = Piece(i, piece_length, self.pieces_SHA1[i])
            self.totalBlocks += piece.number_of_blocks
            self.pieces.append(piece)
    
    def _getSHA1(self):
        for i in range(self.number_of_pieces):
            start = i * 20
            end = start + 20
            self.pieces_SHA1.append(self.torrent.pieces[start : end])

    def _merge_blocks(self, index):
        piece = self.get_piece_safe(index)
        if not piece:
            return None
        return piece.merge_blocks()
        
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
        
    
    def _print_progress_bar_internal(self, suffix, decimals, print_matrix, peers):
        """Internal method for printing progress bar without locks"""
        # Calculate download speed
        out = ""
        out += f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n'
        out += self._progress_bar_string_internal(decimals, suffix) 
        
        if peers is not None:
            out += self._peer_stats_string_internal(peers)   
        
        matrix_data = self._get_pieces_matrix_safe()
        if print_matrix and matrix_data:
            out += self._matrix_string_internal(matrix_data)
        
        try:
            os.makedirs('logs', exist_ok=True)
            os.makedirs(os.path.join('logs', "peermanager"), exist_ok=True)
            with open(os.path.join('logs', "status.log"), 'w', encoding='utf-8') as f:
                f.write(out)
        except Exception as e:
            print(f'error in status: {e}')

    def _progress_bar_string_internal(self, decimals, suffix):
        """Internal method for progress bar string without locks"""
        total = self.totalBlocks
        current_time = time.time()
        time_diff = current_time - self.last_update_time
        if time_diff >= 0.5:
            blocks_done = self.downloaded_blocks
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
            percent = f"{100 * max((self.last_blocks_done / total), (self.num_of_downloaded_pieces()/self.number_of_pieces)):.{decimals}f}"
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
                strings.append(f"Pending: {peer.pending_requests:<3}")
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
        pieces = self.pieces.copy()
        for piece_index, piece in enumerate(pieces):
            piece_status = piece.monitor_block_timeouts(timeout)
            self._fix_sets(piece_status, piece_index)
    
    def getRarestPieceMinHeap(self, connectedPeers: list[Peer]):
        piece_listOfpeers = {}
        for i in range(self.number_of_pieces):
            piece_listOfpeers[i] = []
        for peer in connectedPeers:
            if not peer.connected:
                print("disconnected peer in connected")
            if peer.peer_choking != 0:
                continue
            if peer.bit_field:
                for index in range(len(peer.bit_field)):
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

    # def filter(self, src: list[tuple[int, list[Peer]]], length) -> list[tuple[Piece, list[Peer]]]:
    #     res = []
    #     i = 0
    #     for piece_i, peers in src:
    #         if peers:
    #             piece = self.get_piece_safe(piece_i)
    #             if piece is None or piece.is_complete() or piece.is_requested():
    #                 continue
    #             res.append((piece, peers))
    #             i += 1
    #             if i >= length:
    #                 break
    #     return res
            
    def is_need_to_download(self, piece_index: int):
        res = self.is_piece_empty(piece_index)
        return res
    
    def get_piece_size(self, piece_index: int):
        if piece_index == self.number_of_pieces - 1:
            res = self.torrent.total_length - (self.number_of_pieces - 1) * self.torrent.piece_length
        else:
            res = self.torrent.piece_length
            
        # real = self.get_piece_safe(piece_index).piece_size
        # if res != real:
            # print("get_piece_size")
        return res
    
    def get_blocks_len(self, piece_index: int):
        res = ceil(self.get_piece_size(piece_index) / BLOCK_SIZE)
        # real = self.get_piece_safe(piece_index).number_of_blocks
        # if res != real:
            # print(f"get_blocks_len: params: {piece_index}; res: {res}; real: {real}")
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
        
        # real = self.get_piece_safe(piece_index).blocks[block_index].block_size
        # if res != real:
            # print("get_block_size")
        return res

    def get_piece_safe(self, piece_index: int) -> Piece:
        pieces = self.pieces.copy()
        if 0 <= piece_index < self.number_of_pieces:
            return pieces[piece_index]
        return None

    def get_empty_blocks(self, piece_index):
        piece = self.get_piece_safe(piece_index)
        if not piece:
            return []
        return piece.empty_blocks_list()
    
    def is_index_valid(self, piece_index: int) -> bool:
        return 0 <= piece_index < self.number_of_pieces

    def num_of_downloaded_pieces(self) -> int:
        return self.downloaded_count

    def num_of_requested_pieces(self) -> int:
        return self.requested_count

    def num_of_empty_pieces(self) -> int:
        return self.empty_count

    def is_piece_complete(self, piece_index: int) -> bool:
        return self.statuses[piece_index] is Status.RECEIVED

    def is_piece_requested(self, piece_index: int) -> bool:
        return self.statuses[piece_index] is Status.REQUESTED

    def is_piece_empty(self, piece_index: int) -> bool:
        return self.statuses[piece_index] is Status.EMPTY

    def is_block_empty(self, piece_index, block_index):
        res = self.get_piece_safe(piece_index).is_block_empty(block_index)
        return res

    def all_piece_complete_safe(self):
        return self.num_of_downloaded_pieces() == self.number_of_pieces

    def print_progress_bar_safe(self, suffix='Complete', decimals=1, print_matrix=False, peers=None):
        self._print_progress_bar_internal(suffix, decimals, print_matrix, peers)

    def update_block_status_safe(self, piece_index, block_index, status: Status, data=None, last_requested=None):
        piece: Piece = self.get_piece_safe(piece_index)
        if piece:
            piece_status, fix_blocks = piece.changeStatus(block_index, status, data, last_requested)
            self._fix_sets(piece_status, piece_index)
            if fix_blocks != 0:
                self.downloaded_blocks += fix_blocks
            return True
        return False
    
    def _fix_sets(self, piece_status, piece_index):
        # self._set_status(piece_index, piece_status)
        self._queue.put((piece_index, piece_status))

    def _worker_loop(self):
        while True:
            index, new_status = self._queue.get()
            try:
                self._set_status(index, new_status)
            finally:
                self._queue.task_done()



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
                if not self.is_piece_complete(piece_index):
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
                    if not self.is_piece_complete(master_i):
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
        