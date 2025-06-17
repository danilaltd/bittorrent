from torrent import Torrent
from math import ceil
from BlockandPiece import Piece, BLOCK_SIZE
import random
import colorama
from colorama import Fore, Back, Style
import heapq as hq
import time, shutil
import hashlib
import sys,os
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
            if block.status != 1 or not block.data:
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
        for piece in self.pieces:
            for block in piece.blocks:
                if block.status == 1 : blocksDone += 1
        return blocksDone
            

    def getRarestPieceMinHeap(self, connectedPeers):
        piece_numberOfpeers = {}
        for i in range(self.number_of_pieces):
            piece_numberOfpeers[i] = 0
        for peer in connectedPeers:
            if peer.bit_field:
                for index, piece in enumerate(peer.bit_field):
                    if peer.bit_field[index]:
                        piece_numberOfpeers[index] += 1
        piece_numberOfpeers = (sorted(piece_numberOfpeers.items(), key =
             lambda kv:(kv[1], kv[0])))  
        return piece_numberOfpeers

    def pre_select_next_pieces(self, connected_peers, current_piece_index, num_pieces=5):
        """
        Pre-select next pieces to download based on rarity and availability.
        Returns a list of piece indices that should be downloaded next.
        
        Args:
            connected_peers: List of connected peers
            current_piece_index: Index of the current piece being downloaded
            num_pieces: Number of pieces to pre-select
        """
        # Get rarity information for all pieces
        piece_rarity = {}
        for i in range(self.number_of_pieces):
            if i == current_piece_index or self.pieces[i].is_complete():
                continue
            piece_rarity[i] = 0
            
        # Count how many peers have each piece
        for peer in connected_peers:
            if peer.bit_field:
                for index in piece_rarity:
                    if peer.bit_field[index]:
                        piece_rarity[index] += 1
        
        # Sort pieces by rarity (rarest first) and availability
        sorted_pieces = sorted(piece_rarity.items(), 
                             key=lambda x: (x[1], x[0]))  # Sort by rarity, then by index
        
        # Select top N pieces
        selected_pieces = [piece[0] for piece in sorted_pieces[:num_pieces]]
        
        # Verify that selected pieces are available from at least one peer
        available_pieces = []
        for piece_index in selected_pieces:
            for peer in connected_peers:
                if peer.bit_field and peer.bit_field[piece_index]:
                    available_pieces.append(piece_index)
                    break
        
        return available_pieces

    def printProgressBar(self, iteration, total, prefix='Progress', suffix='Complete', decimals=1, length=100, fill='█', autosize=True, print_matrix=False):
        # Calculate download speed
        current_time = time.time()
        if not hasattr(self, 'last_update_time'):
            self.last_update_time = current_time
            self.last_blocks_done = 0
            self.download_speed = 0
        else:
            time_diff = current_time - self.last_update_time
            if time_diff >= 1.0:  # Update speed every second
                blocks_diff = iteration - self.last_blocks_done
                self.download_speed = blocks_diff * BLOCK_SIZE / time_diff  # bytes per second
                self.last_update_time = current_time
                self.last_blocks_done = iteration

        # Format speed
        if self.download_speed > 1024 * 1024:
            speed_str = f"{self.download_speed/1024/1024:.1f} MB/s"
        elif self.download_speed > 1024:
            speed_str = f"{self.download_speed/1024:.1f} KB/s"
        else:
            speed_str = f"{self.download_speed:.1f} B/s"

        # Calculate ETA
        if self.download_speed > 0:
            remaining_blocks = total - iteration
            eta_seconds = remaining_blocks * BLOCK_SIZE / self.download_speed
            if eta_seconds > 3600:
                eta_str = f"{eta_seconds/3600:.1f}h"
            elif eta_seconds > 60:
                eta_str = f"{eta_seconds/60:.1f}m"
            else:
                eta_str = f"{eta_seconds:.0f}s"
        else:
            eta_str = "∞"

        # Calculate percentage
        percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
        
        # Format the progress bar
        styling = '%s |%s| %s%% %s' % (prefix, fill, percent, suffix)
        if autosize:
            cols, _ = shutil.get_terminal_size(fallback=(length, 1))
            length = cols - len(styling) - len(speed_str) - len(eta_str) - 10  # Extra space for speed and ETA
        
        filledLength = int(length * iteration // total)
        bar = fill * filledLength + '-' * (length - filledLength)
        
        if print_matrix:
            # Create block state matrix
            matrix_width = 100
            matrix = []
            current_row = []
            
            for piece in self.pieces:
                for block in piece.blocks:
                    if block.status == 1:
                        current_row.append('■')  # Downloaded
                    elif block.last_requested:
                        current_row.append('▣')  # Currently downloading
                    else:
                        current_row.append('□')  # Not downloaded
                    
                    if len(current_row) == matrix_width:
                        matrix.append(''.join(current_row))
                        current_row = []
            
            if current_row:  # Add remaining blocks
                matrix.append(''.join(current_row) + '□' * (matrix_width - len(current_row)))
            
            # Print the progress bar with speed and ETA
            print('%s %s ETA: %s' % (styling.replace(fill, bar), speed_str, eta_str))
            
            # Print block state matrix
            print('\nBlock States:')
            for row in matrix:
                print(row)
        else:
            print('\r%s %s ETA: %s' % (styling.replace(fill, bar), speed_str, eta_str), end='\r')
        
        # Print new line on complete
        if iteration == total:
            print()