from Messages import keep_alive
from peer import Peer
from tracker import Tracker
from PeerManager import PeerManager
from BlockandPiece import Piece, BLOCK_SIZE, BlockStatus
import threading
import time
import sys
import os
from logger import Logger, timed_lock, print_lock_stats

class Bittorrent:
    def __init__(self) -> None:
        self.max_concurrent_blocks = 50
        self.semaphore = threading.Semaphore(self.max_concurrent_blocks)
        self._progress_thread = None
        self._monitor_thread = None
        self.running = True

    def _clear_logs_directory(self):
        """Очищает содержимое папки logs при запуске"""
        logs_dir = "logs"
        if os.path.exists(logs_dir):
            try:
                for filename in os.listdir(logs_dir):
                    file_path = os.path.join(logs_dir, filename)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        Logger.info(f"Удален лог файл: {filename}")
                Logger.info("Папка logs очищена")
            except Exception as e:
                Logger.error(f"Ошибка при очистке папки logs: {e}")
        else:
            Logger.info("Папка logs не найдена, создание не требуется")

    def start_downloading(self, torrent, path):
        self._clear_logs_directory()
        self.tracker = Tracker(torrent, path)
        self.peer_manager = PeerManager(self.tracker)
        self.file_thread = threading.Thread(target=self.peer_manager.piece_manager.write_into_file_safe)
        
        Logger.info(f"Total torrent length: {self.tracker.torrent_obj.total_length}")

        try:
            self._initialize()
            self._download_loop()
            self._notify_trackers_complete()
        finally:
            self._finalize()

    def _initialize(self):
        self.tracker.start_periodic_updates()
        self.file_thread.start()

    def _download_loop(self):
        # Start monitor thread
        self._monitor_thread = threading.Thread(target=self.monitor_block_timeouts)
        self._monitor_thread.daemon = True
        self._monitor_thread.start()
        
        # Start progress thread
        self._progress_thread = threading.Thread(target=self.progress_printer)
        self._progress_thread.daemon = True
        self._progress_thread.start()
        
        while not self.peer_manager.piece_manager.all_piece_complete_safe():
            try:
                print("loop")
                self._update_stats()
                self.peer_manager.update_optimistic_unchoke()
                self._download_rarest_first()
            except Exception as e:
                Logger.error(f"Error in download loop: {e}")
                time.sleep(1)  # Небольшая пауза перед повторной попыткой
        
        # Wait for progress thread to finish
        # if self._progress_thread and self._progress_thread.is_alive():
            # self._progress_thread.join()

    def _download_rarest_first(self):
        try:
            min_heap: list[tuple[int, list[Peer]]] = self.peer_manager.get_rarest_piece_min_heap_copy()
            # min_heap: list[tuple[Piece, list[Peer]]] = self.peer_manager.piece_manager.filter(raw_heap, 150)
            threads = []
            i = 0
            # print("append start")
            for piece_i, peers in min_heap:
                if i >= 150:
                    break
                if not peers:
                    Logger.info("No peers available. Retrying...")
                    time.sleep(1)
                    break
                if not self.peer_manager.piece_manager.is_need_to_download(piece_i):
                    continue
                i+=1
                try:
                    thread = threading.Thread(target=self._request_piece_from_peers, args=(piece_i, peers))
                    thread.daemon = True
                    threads.append(thread)
                except Exception as e:
                    Logger.error(f"Error creating thread for piece {piece_i}: {e}")
                    continue
            if threads:
                for thread in threads:
                    try:
                        thread.start()
                    except Exception as e:
                        Logger.error(f"Error starting thread: {e}")
                        continue
                # for thread in threads:
                #     try:
                #         thread.join(timeout=30)
                #     except Exception as e:
                #         Logger.error(f"Error joining thread: {e}")
        except Exception as e:
            Logger.error(f"Error in _download_rarest_first: {e}")

    def _request_piece_from_peers(self, piece_index, peers: list[Peer]):
        print(f"_request_piece_from_peers for {piece_index}")
        try:
            if not peers:
                return
            
            available_peers = peers           
            peer = None
            try:
                peer = self.peer_manager.get_best_peer(available_peers)
            except Exception as e:
                Logger.error(f"Error getting best peer: {e}")
                return
            
            if not peer:
                Logger.info(f"Could not select best peer for piece {piece_index}")
                return
            
            try:
                if not peer.connected:
                    Logger.info(f"Selected peer {peer.ip_port} is no longer valid: not connected now")
                    return
                if not peer.sock:
                    Logger.info(f"Selected peer {peer.ip_port} is no longer valid: socket is bad")
                    return
                if not peer.is_socket_valid():
                    Logger.info(f"Selected peer {peer.ip_port} is no longer valid: invalid socket")
                    return
            except Exception as e:
                Logger.error(f"Error validating selected peer: {e}")
                return
            
            self._request_blocks(peer, piece_index)
            
        except Exception as e:
            Logger.error(f"Error in _request_piece_from_peers for piece {piece_index}: {e}")

    def _request_blocks(self, peer: Peer, piece_index, max_blocks=16):
        try:
            print(f"_request_blocks for {piece_index}")
            for block_index in range(min(max_blocks, self.peer_manager.piece_manager.get_blocks_len(piece_index))):
                # Get piece safely to check block status
                piece_safe = self.peer_manager.piece_manager.get_piece_safe(piece_index)
                if not piece_safe:
                    continue
                    
                block = piece_safe.blocks[block_index]
                if block.status != BlockStatus.EMPTY:
                    continue
                    
                # Update block status to REQUESTED safely
                self.peer_manager.piece_manager.update_block_status_safe(
                    piece_index, block_index, BlockStatus.REQUESTED
                )
                
                try:
                    with timed_lock(self.semaphore, "concurrent_blocks_semaphore"):
                        # Получаем запрос на блок
                        request_block = None
                        try:
                            request_block = self.peer_manager.request_blockByteString(piece_index, block_index, block)
                        except Exception as e:
                            Logger.error(f"Error creating request block: {e}")
                            self.peer_manager.piece_manager.update_block_status_safe(
                                piece_index, block_index, BlockStatus.EMPTY
                            )
                            break
                        
                        if not request_block:
                            Logger.error(f"Failed to create request block for piece {piece_index}, block {block_index}")
                            self.peer_manager.piece_manager.update_block_status_safe(
                                piece_index, block_index, BlockStatus.EMPTY
                            )
                            break
                        
                        # Добавляем keepalive если нужно
                        try:
                            if time.time() - peer.last_transmission > 60:
                                request_block += keep_alive.byteStringForKeepAlive()
                                peer.last_transmission = time.time()
                        except Exception as e:
                            Logger.error(f"Error adding keepalive: {e}")

                        peer.requests_sent += 1
                        peer.pending_requests += 1
                        peer.last_request_time = time.time()
                        
                        # Безопасная отправка данных через сокет
                        try:
                            # Используем новый метод для безопасной отправки
                            print(f"send req for {piece_index}")
                            if not peer.send_data(request_block):
                                raise Exception("Failed to send data to peer")
                            
                            # Update last_requested safely (вне блокировки peer)
                            self.peer_manager.piece_manager.update_block_status_safe(
                                piece_index, block_index, BlockStatus.REQUESTED,
                                last_requested=time.time()
                            )
                        except Exception as sock_error:
                            peer.pending_requests = max(0, peer.pending_requests - 1)
                            self.peer_manager.piece_manager.update_block_status_safe(
                                piece_index, block_index, BlockStatus.EMPTY
                            )
                            Logger.error(f"Socket error for {peer.ip_port}: {sock_error}")
                            try:
                                if self.peer_manager.is_peer_in_connected_peers(peer):
                                    self.peer_manager._remove_connected_peer(peer)
                            except Exception as e:
                                Logger.error(f"Error removing peer after socket error: {e}")
                            break

                        # Префетч следующих блоков
                        try:
                            self.peer_manager.prefetch_next_blocks(peer.sock, piece_index, block_index, peer)
                        except Exception as e:
                            Logger.error(f"Error prefetching next blocks: {e}")

                except Exception as e:
                    self.peer_manager.piece_manager.update_block_status_safe(
                        piece_index, block_index, BlockStatus.EMPTY
                    )
                    try:
                        if self.peer_manager.is_peer_in_connected_peers(peer):
                            self.peer_manager._remove_connected_peer(peer)
                    except Exception as remove_error:
                        Logger.error(f"Error removing peer after exception: {remove_error}")
                    Logger.error(f"Error requesting block from {peer.ip_port}: {e}")
                    break
        except Exception as e:
            Logger.error(f"Error in _request_blocks for peer {peer.ip_port}: {e}")

    def _update_stats(self):
        try:
            downloaded, blocks_in_progress = self.peer_manager.piece_manager.download_blocks_safe()
            uploaded = sum(peer.uploaded for peer in self.peer_manager.get_connected_peers_for_stats())
            self.tracker.update_stats(downloaded * BLOCK_SIZE, uploaded)
        except Exception as e:
            Logger.error(f"Error updating stats: {e}")

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
            self.peer_manager.piece_manager.download_blocks_safe()[0],
            self.peer_manager.piece_manager.totalBlocks,
            f"Completed {self.peer_manager.piece_manager.pieces_downloaded_safe()}/{len(self.peer_manager.piece_manager.pieces)}"
        )

    def _finalize(self):
        self.running = False
        self.tracker.stop_periodic_updates()
        self.peer_manager.exitPeerThreads()
        self.file_thread.join()
        
    def monitor_block_timeouts(self, check_interval=5, timeout=10):
        while self.running:
            self.peer_manager.piece_manager.monitor_block_timeouts_safe(timeout)
            time.sleep(check_interval)

    def progress_printer(self):
        while not self.peer_manager.piece_manager.all_piece_complete_safe() and self.running:
            downloaded, blocks_in_progress = self.peer_manager.piece_manager.download_blocks_safe()
            self.peer_manager.piece_manager.print_progress_bar_safe(
                downloaded,
                self.peer_manager.piece_manager.totalBlocks,
                f"{self.peer_manager.piece_manager.pieces_downloaded_safe()}/{len(self.peer_manager.piece_manager.pieces)}",
                print_matrix=True,
                peers=self.peer_manager.get_peers_for_progress()
            )
            time.sleep(1)
            
    def print_lock_statistics(self):
        """Выводит статистику использования locks для всего приложения"""
        print_lock_stats()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python main.py <torrent_file> <download_path>")
        sys.exit(1)
    torrent = sys.argv[1]
    path = sys.argv[2]
    b = Bittorrent()
    b.start_downloading(torrent, path)