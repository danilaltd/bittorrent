from Messages import keep_alive
from peer import Peer
from tracker import Tracker
from PeerManager import PeerManager
from BlockandPiece import BLOCK_SIZE, Status
import threading
import time
from datetime import datetime
import sys
import os
import random
from logger import Logger, timed_lock, print_lock_stats

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
        self.tracker = Tracker(torrent, path)
        self.peer_manager = PeerManager(self.tracker, self.set_updated)
        
        Logger.info(f"Total torrent length: {self.tracker.torrent_obj.total_length}")

        try:
            self._initialize()
            try:
                self._download_loop()
            except Exception as e:
                while True:
                    print(e)
            self._notify_trackers_complete()
        finally:
            self._finalize()

    def _initialize(self):
        self.tracker.start_periodic_updates()
        # self.file_thread = threading.Thread(target=self.peer_manager.piece_manager.write_into_file_safe)
        # self.file_thread.start()

    def _download_loop(self):
        # Start monitor thread
        self._monitor_thread = threading.Thread(target=self.monitor_block_timeouts)
        self._monitor_thread.daemon = True
        self._monitor_thread.start()
        
        # Start progress thread
        self._progress_thread = threading.Thread(target=self.progress_printer)
        self._progress_thread.daemon = True
        self._progress_thread.start()
        
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
                    # else:
                        # print("skip")
            if not sent:
                Logger.info("No peers available. Retrying...")
                time.sleep(1)
        except Exception as e:
            log_error(f"Error in _download_rarest_first: {e}")

    def _update_stats(self):
        try:
            downloaded = self.peer_manager.piece_manager.num_of_downloaded_pieces()
            uploaded = sum(peer.uploaded for peer in self.peer_manager.get_connected_peers_for_stats())
            self.tracker.update_stats(downloaded * BLOCK_SIZE, uploaded)
        except Exception as e:
            log_error(f"Error updating stats: {e}")

    def _notify_trackers_complete(self):
        Logger.info("Torrent Complete")
        Logger.info(f"Total length: {self.tracker.torrent_obj.total_length}")
        self.peer_manager.torrent_completed = True
        for url in self.tracker.torrent_obj.announce_list:
            if "http" in url:
                threading.Thread(target=self.tracker.http_request, args=(url, 'completed')).start()
            elif "udp" in url:
                threading.Thread(target=self.tracker.udp_request, args=(url, 'completed')).start()

        self.peer_manager.piece_manager.print_progress_bar_safe(
            f"Completed {self.peer_manager.piece_manager.num_of_downloaded_pieces()}/{self.peer_manager.piece_manager.num_of_requested_pieces()}/{self.peer_manager.piece_manager.num_of_empty_pieces()}/{self.peer_manager.piece_manager.number_of_pieces}"
        )

    def _finalize(self):
        self.running = False
        self.tracker.stop_periodic_updates()
        self.peer_manager.exitPeerThreads()
        self.file_thread.join()
        
    def monitor_block_timeouts(self, check_interval=5, timeout=15):
        time.sleep(100000)
        while self.running:
            self.peer_manager.piece_manager.monitor_block_timeouts_safe(timeout)
            time.sleep(check_interval)

    def progress_printer(self):
        # not self.peer_manager.piece_manager.all_piece_complete_safe() and
        while self.running:
            self.peer_manager.piece_manager.print_progress_bar_safe(
                f"{self.peer_manager.piece_manager.num_of_downloaded_pieces()}/{self.peer_manager.piece_manager.num_of_requested_pieces()}/{self.peer_manager.piece_manager.num_of_empty_pieces()}/{self.peer_manager.piece_manager.number_of_pieces}",
                print_matrix=True,
                peers=self.peer_manager.get_peers_for_progress()
            )
            time.sleep(1)
            
    def print_lock_statistics(self):
        """Выводит статистику использования locks для всего приложения"""
        print_lock_stats()

if __name__ == "__main__":
    # if len(sys.argv) != 3:
        # print("Usage: python main.py <torrent_file> <download_path>")
        # sys.exit(1)
    # torrent = sys.argv[1]
    # path = sys.argv[2]

    # torrent = r'.\torrents\music.torrent'
    # path = r"./down"
    
    torrent = os.path.join('torrents', 'The_Jackbox_Party_Pack_3_MANY_PEERS_680MB.torrent')
    # torrent = os.path.join('torrents', 'music.torrent')
    path = os.path.join('.', 'down')
    b = Bittorrent()
    b.start_downloading(torrent, path)
    
    