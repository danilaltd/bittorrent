import threading
import time
from BlockandPiece import BLOCK_SIZE, Piece, BlockStatus
from logger import Logger

class PieceManager:
    def __init__(self, torrent_obj):
        self.torrent_obj = torrent_obj
        self.pieces = []
        self.total_blocks = 0
        self.initialize_pieces()
        self.write_lock = threading.Lock()
        self.file_thread = None

    def initialize_pieces(self):
        piece_length = self.torrent_obj.piece_length
        total_length = self.torrent_obj.total_length
        self.number_of_pieces = (total_length + piece_length - 1) // piece_length

        for i in range(self.number_of_pieces):
            piece = Piece(i, piece_length, total_length)
            self.pieces.append(piece)
            self.total_blocks += len(piece.blocks)

    def get_rarest_piece_min_heap(self, connected_peers):
        piece_counts = [0] * len(self.pieces)
        
        for peer in connected_peers:
            for i, has_piece in enumerate(peer.bit_field):
                if has_piece and not self.pieces[i].is_complete():
                    piece_counts[i] += 1

        # Create min heap of (piece_index, count) tuples
        min_heap = [(i, count) for i, count in enumerate(piece_counts) if count > 0]
        min_heap.sort(key=lambda x: x[1])
        return min_heap

    def download_blocks(self):
        return sum(1 for piece in self.pieces for block in piece.blocks if block.status == BlockStatus.RECEIVED)

    def pieces_downloaded(self):
        return sum(1 for piece in self.pieces if piece.is_complete())

    def all_piece_complete(self):
        return all(piece.is_complete() for piece in self.pieces)

    def write_into_file(self):
        with self.write_lock:
            try:
                with open(self.torrent_obj.name, 'wb') as f:
                    for piece in self.pieces:
                        if piece.is_complete():
                            for block in piece.blocks:
                                f.write(block.data)
            except Exception as e:
                Logger.error("Error writing to file", e)

    def print_progress_bar(self, current, total, prefix='', suffix='', print_matrix=False):
        bar_length = 50
        filled_length = int(round(bar_length * current / float(total)))
        percents = round(100.0 * current / float(total), 1)
        bar = '=' * filled_length + '-' * (bar_length - filled_length)
        
        if print_matrix:
            print('\r%s [%s] %s%% %s' % (prefix, bar, percents, suffix), end='')
        else:
            print('\r%s [%s] %s%% %s' % (prefix, bar, percents, suffix), end='')
        if current == total:
            print()