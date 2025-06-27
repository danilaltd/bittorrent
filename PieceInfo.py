from torrent import Torrent
from math import ceil
from BlockandPiece import Piece, BLOCK_SIZE, BlockStatus
import random
import colorama
from colorama import Fore, Back, Style
import heapq as hq
import time, shutil
import hashlib
import sys,os
from itertools import groupby
colorama.init(autoreset=True)#auto resets your settings after every output

class PieceInfo:
    def __init__(self, torrent):
        self.torrent:Torrent = torrent
        self.number_of_pieces = ceil(torrent.total_length / torrent.piece_length)
        self.pieces = []
        self.pieces_SHA1 = []
        self.getSHA1()
        self.generate_piece()
        self.totalBlocks = self.getTotalBlocks()
        self.files = self._load_files()
    def generate_piece(self):
        last_piece = self.number_of_pieces - 1
        for i in range(self.number_of_pieces):
            if i == last_piece:
                piece_length = self.torrent.total_length - (self.number_of_pieces - 1) * self.torrent.piece_length
                self.pieces.append(Piece(i, piece_length, self.pieces_SHA1[i]))
            else:
                self.pieces.append(Piece(i, self.torrent.piece_length, self.pieces_SHA1[i]))
    
    def getSHA1(self):
        for i in range(self.number_of_pieces):
            start = i * 20
            end = start + 20
            self.pieces_SHA1.append(self.torrent.pieces[start : end])

    def merge_blocks(self, index):
        res = b""
        for block in self.pieces[index].blocks:
            if block.status != BlockStatus.RECEIVED or not block.data:
                return None
            res += block.data
        return res

    def all_piece_complete(self):
        for piece in self.pieces:
            if not piece.is_complete() or not piece.verify_piece():
                return False
        return True
    
    def getRandomPiece(self):
        piece = None
        while True:
            piece : Piece = random.choice(self.pieces)
            if piece.is_complete() == False:
                return piece
        
    def _load_files(self):
        files = []
        piece_offset = 0
        piece_size_used = 0

        for f in self.torrent.files:
            current_size_file = f["length"]
            file_offset = 0

            while current_size_file > 0:
                id_piece = int(piece_offset / self.torrent.piece_length)
                piece_size = self.pieces[id_piece].piece_size - piece_size_used

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
    def write_into_file(self):
        master_i = 0
        n = len(self.files)
        if self.torrent.multipleFiles:
            while master_i < n:
                piece_index = self.files[master_i]['idPiece']
                length = self.files[master_i]['length']
                file_offset = self.files[master_i]['fileOffset']
                piece_offset = self.files[master_i]['pieceOffset']
                path = self.files[master_i]['path']

                if not self.pieces[piece_index].is_complete() or not self.pieces[piece_index].verify_piece():
                    continue
                
                try:
                    # Create full path using os.path.join
                    path_to_file = os.path.join(self.torrent.total_path, *path)
                    
                    # Create parent directories if they don't exist
                    os.makedirs(os.path.dirname(path_to_file), exist_ok=True)
                    
                    # Try to open existing file first
                    try:
                        f = open(path_to_file, 'r+b')
                    except IOError:
                        f = open(path_to_file, 'wb')
                    
                    piece_data = self.merge_blocks(piece_index)
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
                    if not self.pieces[master_i].is_complete() or not self.pieces[master_i].verify_piece():
                        continue
                    
                    piece_data = self.merge_blocks(master_i)
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
    
    def piecesDownloaded(self):
        done = 0
        for piece in self.pieces:
            if piece.is_complete(): done += 1
        return done
    def getTotalBlocks(self):
        total = 0
        for piece in self.pieces:
            total += len(piece.blocks)
        return total
    def downloadBlocks(self):
        blocksDone = 0
        blocksInProgress = 0
        for piece in self.pieces:
            for block in piece.blocks:
                if block.status == BlockStatus.RECEIVED:
                    blocksDone += 1
                elif block.last_requested and time.time() - block.last_requested < 30:  # Consider blocks requested in last 30 seconds as in progress
                    blocksInProgress += 1
        return blocksDone, blocksInProgress

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
        result = []
        for _, group in groupby(piece_listOfpeers, key=lambda kv: len(kv[1])):
            group_list = list(group)
            random.shuffle(group_list)
            result.extend(group_list)
        for _, peers in result:
            peers.sort(key=lambda peer: peer.peer_score(), reverse=True) 
        return result

    def progressBarString(self, iteration, total, decimals=1, suffix='Complete'):
        current_time = time.time()
        if not hasattr(self, 'last_update_time'):
            self.last_update_time = current_time
            self.last_blocks_done = 0
            self.last_bytes_done = 0
            self.download_speed = 0
            self.speed_history = []  # Store last 5 speed measurements
        else:
            time_diff = current_time - self.last_update_time
            if time_diff >= 0.5:  # Update speed more frequently
                blocks_done, blocks_in_progress = self.downloadBlocks()
                current_bytes = blocks_done * BLOCK_SIZE
                bytes_diff = current_bytes - self.last_bytes_done
                
                # Calculate current speed
                current_speed = bytes_diff / time_diff  # bytes per second
                
                # Add current speed to history
                self.speed_history.append(current_speed)
                if len(self.speed_history) > 5:
                    self.speed_history.pop(0)
                
                # Calculate average speed from history
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

    def matrixString(self):
        res = ''
        matrix_width = 50
        matrix = []
        current_row = []
        
        for piece in self.pieces:
            if piece.is_complete():
                current_row.append('*')  # Downloaded
            elif piece.is_requested():  # Only show active downloads from last 30 seconds
                current_row.append('-')  # Currently downloading
            else:
                current_row.append(' ')  # Not downloaded
            
            if len(current_row) == matrix_width:
                matrix.append(''.join(current_row))
                current_row = []
        
        if current_row:  # Add remaining blocks
            matrix.append(''.join(current_row) + ' ' * (matrix_width - len(current_row)))
        
        
        # Print block state matrix
        res += f'\nWidth: {matrix_width}\n'
        res += '\nBlock States:\n'
        for row in matrix:
            res += f"{row}\n"
        return res
        
    def peerStatsString(self, peers):
        res = ''
        try:
            res += '\nPeers: %d' % len(peers)
            needed_pieces = set(i for i, p in enumerate(self.pieces) if not p.is_complete())
            # Сортируем: сначала подключённые, потом нет
            sorted_peers = sorted(peers, key=lambda peer: (not peer.connected, -peer.blocks_recieved))
            # Для быстрого определения активности
            def is_active(peer):
                if hasattr(peer, 'bit_field') and peer.bit_field:
                    return any(peer.bit_field[i] for i in needed_pieces if i < len(peer.bit_field))
                return False
            active_peers = sum(1 for peer in sorted_peers if is_active(peer))
            res += f' (active: {active_peers})\n'
            # Краткая инфа по каждому пиру
            border = False
            if sorted_peers:
                res += "Connected:\n"
            for idx, peer in enumerate(sorted_peers):
                if not border and not peer.connected:
                    res += "Not conected:\n"
                    border = True
                peer_id = getattr(peer, 'id', None) or getattr(peer, 'ip', None) or f'peer{idx+1}'
                available = 0
                if hasattr(peer, 'bit_field') and peer.bit_field:
                    available = sum(1 for i in needed_pieces if i < len(peer.bit_field) and peer.bit_field[i])
                # speed = peer.rate
                # speed_str_peer = f', speed: {speed/1024:.1f} KB/s' if speed else ''
                active = is_active(peer)
                sign = '+' if active else '-'
                res += f'  {sign} {peer_id}: {available} pieces; Blocks recieved: {peer.blocks_recieved}; Connections: {peer.requests_sent}; Pending: {peer.pending_requests}\n'
        except Exception as e:
            res += f'\n[Peer stats error: {e}]\n'
            
        return res
    
    def printProgressBar(self, iteration, total, prefix='Progress', suffix='Complete', decimals=1, length=100, fill='█', autosize=True, print_matrix=False, peers=None):
        # Calculate download speed
        out = ""
        out += self.progressBarString(iteration, total, decimals, suffix) 
        
        if peers is not None:
            out += self.peerStatsString(peers)   
        
        if print_matrix:
            out += self.matrixString()
        
        try:
            os.makedirs('logs', exist_ok=True)
            with open(os.path.join('logs', "status.log"), 'w', encoding='utf-8') as f:
                f.write(out)
        except Exception as e:
            print(f'error in status: {e}')