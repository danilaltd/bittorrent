from peer import Peer
from TrackersManager import TrackersManager
from PeerManager import PeerManager
from BlockandPiece import BLOCK_SIZE
import threading
import time
from datetime import datetime
import os
import yappi
from logger import Logger, print_lock_stats

NEG_INF = float('-inf')

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
        path = os.path.join('logs', 'main')
    else:
        name = 'main.log'
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
        path = os.path.join('logs', 'main')
    else:
        name = 'main.log'
        path = 'logs'
        
    with open(os.path.join(f'{path}', f"{name}"), 'a', encoding='utf-8') as f:
        f.write(res)

class Bittorrent:
    def __init__(self) -> None:
        self.max_concurrent_blocks = 150000
        self.semaphore = threading.Semaphore(self.max_concurrent_blocks)
        self._progress_thread = None
        self._monitor_thread = None
        self.running = True
        self.min_heap_updated = False

    def _clear_logs_directory(self):
        """Очищает содержимое папки logs при запуске"""
        import shutil
        logs_dir = "logs"
        if os.path.exists(logs_dir):
            try:
                for filename in os.listdir(logs_dir):
                    file_path = os.path.join(logs_dir, filename)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        Logger.info(f"Удален лог файл: {filename}")
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                        Logger.info(f"Удалена папка с логами: {filename}")
                Logger.info("Папка logs очищена")
            except Exception as e:
                Logger.error(f"Ошибка при очистке папки logs: {e}")
        else:
            Logger.info("Папка logs не найдена, создание не требуется")
        os.makedirs('logs', exist_ok=True)
        os.makedirs(os.path.join('logs', "peermanager"), exist_ok=True)
    
    def start_downloading(self, torrent, path):
        self._clear_logs_directory()
        self.tracker = TrackersManager(torrent, path)
        self.peer_manager = PeerManager(self.tracker, self.set_updated)
        
        Logger.info(f"Total torrent length: {self.tracker.torrent_obj.total_length}")

        try:
            self._initialize()
            try:
                self._download_loop()
            except Exception as e:
                log_error(f"self._download_loop(): {e}")
            self._notify_trackers_complete()
        finally:
            self._finalize()

    def _initialize(self):
        self.tracker.start_periodic_updates()

    def _download_loop(self):
        # Start monitor thread
        # Start progress thread
        
        # while not self.peer_manager.piece_manager.all_piece_complete_safe():
        while True:
            try:
                print("loop")
                # self._update_stats()
                # self.peer_manager.update_optimistic_unchoke()
                self._download_rarest_first()
            except Exception as e:
                log_error(f"Error in download loop: {e}")
                time.sleep(1)  # Небольшая пауза перед повторной попыткой
        
        # Wait for progress thread to finish
        # if self._progress_thread and self._progress_thread.is_alive():
            # self._progress_thread.join()

    def set_updated(self):
        self.min_heap_updated = True

    def _download_rarest_first(self):
        try:
            min_heap: list[tuple[int, list[Peer]]] = self.peer_manager.get_rarest_piece_min_heap_copy()
            i = 0
            sent = False
            if min_heap and min_heap[0][1]:
                sent = True
                for piece_i, peers in min_heap:
                    if self.min_heap_updated:
                        self.min_heap_updated = False
                        break
                    # best_peer = peers[0]
                    # scored_peers = [(peer.peer_score(), peer) for peer in peers]
                    # best_score, best_peer = max(scored_peers, key=lambda x: x[0], default=(NEG_INF, None))
                    best_peer = max(peers, key=lambda p: p.peer_score(), default=None)
                    # print()
                    if best_peer and best_peer.peer_score() > NEG_INF:
                        i += self.peer_manager.prefetch_next_blocks(piece_i, best_peer)
                    else:
                        time.sleep(0.5)
            if not sent or i == 0:
                Logger.info("No peers available. Retrying...")
                time.sleep(1)
        except Exception as e:
            log_error(f"Error in _download_rarest_first: {e}")

    def _update_stats(self):
        try:
            downloaded = self.peer_manager.piece_manager.num_of_downloaded_pieces()
            uploaded = sum(peer.uploaded for peer in self.peer_manager.get_connected_peers_copy())
            self.tracker.update_stats(downloaded * BLOCK_SIZE, uploaded)
        except Exception as e:
            log_error(f"Error updating stats: {e}")

    def _notify_trackers_complete(self):
        Logger.info("Torrent Complete")
        Logger.info(f"Total length: {self.tracker.torrent_obj.total_length}")
        self.peer_manager.torrent_completed = True
        self.tracker.notify_trackers_complete()
        # self.peer_manager.piece_manager.print_progress_bar_safe(
        #     f"Completed {self.peer_manager.piece_manager.num_of_downloaded_pieces()}/{self.peer_manager.piece_manager.num_of_requested_pieces()}/{self.peer_manager.piece_manager.num_of_empty_pieces()}/{self.peer_manager.piece_manager.number_of_pieces}"
        # )

    def _finalize(self):
        self.running = False
        self.tracker.stop_periodic_updates()
        self.peer_manager.exitPeerThreads()
            
    def print_lock_statistics(self):
        """Выводит статистику использования locks для всего приложения"""
        print_lock_stats()

def main():
    # if len(sys.argv) != 3:
        # print("Usage: python main.py <torrent_file> <download_path>")
        # sys.exit(1)
    # torrent = sys.argv[1]
    # path = sys.argv[2]

    # torrent = r'.\torrents\music.torrent'
    # path = r"./downloads"
    
    # torrent = os.path.join('torrents', 'The_Jackbox_Party_Pack_3_MANY_PEERS_680MB.torrent')
    # torrent = os.path.join('torrents', 'REPO_300.torrent')
    # torrent = os.path.join('torrents', 'music.torrent')
    # torrent = os.path.join('torrents', 'manyLeeches5.torrent')
    torrent = os.path.join('torrents', 'Andr.torrent')
    # torrent = os.path.join('torrents', 'ninja.torrent')
    # torrent = os.path.join('torrents', 'FoxLake.torrent')
    # torrent = os.path.join('torrents', '245_rut.torrent')
    # torrent = os.path.join('torrents', 'Photoshop_4gb.torrent')
    # torrent = os.path.join('torrents', 'Photoshop_2.58gb_rutr.torrent')
    # torrent = os.path.join('torrents', '1PieceManyManyFiles.torrent')
    path = os.path.join('.', 'downloads')
    b = Bittorrent()
    b.start_downloading(torrent, path)

if __name__ == "__main__":
    yappi.set_clock_type("cpu")
    yappi.start()
    try:
        main()
    finally:
        yappi.stop()
        yappi.get_func_stats().save("profile/profile.callgrind", type="CALLGRIND")
        yappi.get_func_stats().save("profile/profile.pstat", type="pstat")


        # yappi.get_func_stats().save("profile/yappi.prof", type="pstat")