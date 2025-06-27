from Messages import keep_alive
from peer import Peer
from torrent import Torrent
from tracker import Tracker
from PeerManager import PeerManager
from BlockandPiece import Piece, BLOCK_SIZE, BlockStatus
import threading
import time
import sys
from logger import Logger
import asyncio

class Bittorrent:
    def __init__(self) -> None:
        self.max_concurrent_blocks = 50
        self.semaphore = asyncio.Semaphore(self.max_concurrent_blocks)
        self._progress_task = None

    def start_downloading(self, torrent, path):
        self.tracker = Tracker(torrent, path)
        self.peer_manager = PeerManager(self.tracker)
        self.file_thread = threading.Thread(target=self.peer_manager.piece_manager.write_into_file)
        
        Logger.info(f"Total torrent length: {self.tracker.torrent_obj.total_length}")

        try:
            self._initialize()
            asyncio.run(self._download_loop_async())
            self._notify_trackers_complete()
        finally:
            self._finalize()

    def _initialize(self):
        self.tracker.start_periodic_updates()
        self.file_thread.start()

    async def _download_loop_async(self):
        asyncio.create_task(self.monitor_block_timeouts())
        self._progress_task = asyncio.create_task(self.progress_printer())
        while not self.peer_manager.piece_manager.all_piece_complete():
            print("loop")
            self._update_stats()
            self.peer_manager.update_optimistic_unchoke()
            await self._download_rarest_first_async()
        if self._progress_task:
            await self._progress_task

    async def _download_rarest_first_async(self):
        min_heap = self.peer_manager.rarest_piece_min_heap
        tasks = []
        i = 0
        print("append start")
        for item in min_heap:
            if i >= 50:
                break
            piece = self.peer_manager.piece_manager.pieces[item[0]]
            peers = item[1]
            if not peers:
                Logger.info("No peers available. Retrying...")
                await asyncio.sleep(1)
                break
            if not piece.is_complete() and not piece.is_requested():
                i += 1
                # print(f"req {piece.piece_index}")
                tasks.append(self._request_piece_from_peers_async(piece, peers))
        print("append end")
        if tasks:
            print("tasks start")
            await asyncio.gather(*tasks)
            print("tasks completed")

    async def _request_piece_from_peers_async(self, piece, peers):
        if not peers:
            Logger.info("No peers available. Retrying...")
            await asyncio.sleep(1)
            return
        peer = self.peer_manager.get_best_peer(peers)
        if not peer or peer.peer_choking:
            return
        await self._request_blocks_async(peer, piece)

    async def _request_blocks_async(self, peer, piece, max_blocks=16):
        for block_index in range(min(max_blocks, len(piece.blocks))):
            block = piece.blocks[block_index]
            if block.status != BlockStatus.EMPTY:
                continue
            block.status = BlockStatus.REQUESTED
            try:
                async with self.semaphore:
                    request_block = self.peer_manager.request_blockByteString(piece, block_index, block)
                    
                    if time.time() - peer.last_transmission > 60:
                        request_block += keep_alive.byteStringForKeepAlive()
                        peer.last_transmission = time.time()

                    loop = asyncio.get_running_loop()
                    peer.requests_sent += 1
                    peer.pending_requests += 1
                    peer.last_request_time = time.time()
                    try:
                        await loop.sock_sendall(peer.sock, request_block)
                    except:
                        peer.pending_requests = max(0, peer.pending_requests - 1)
                    peer.last_transmission = time.time()
                    block.last_requested = time.time()

                    self.peer_manager.prefetch_next_blocks(peer.sock, piece, block_index, peer)

            except Exception as e:
                block.status = BlockStatus.EMPTY
                if peer in self.peer_manager.connected_peers:
                    self.peer_manager.connected_peers.remove(peer)
                Logger.error(f"Error requesting block from {peer.ip_port}", e)
                break

    def _update_stats(self):
        downloaded, blocks_in_progress = self.peer_manager.piece_manager.downloadBlocks()
        uploaded = sum(peer.uploaded for peer in self.peer_manager.connected_peers)
        self.tracker.update_stats(downloaded * BLOCK_SIZE, uploaded)

    def _notify_trackers_complete(self):
        Logger.info("Torrent Complete")
        Logger.info(f"Total length: {self.tracker.torrent_obj.total_length}")
        self.peer_manager.torrent_completed = True

        for url in self.tracker.torrent_obj.announce_list:
            if "http" in url:
                threading.Thread(target=self.tracker.http_request, args=(url, 'completed')).start()
            elif "udp" in url:
                threading.Thread(target=self.tracker.udp_request, args=(url, 'completed')).start()

        self.peer_manager.piece_manager.printProgressBar(
            self.peer_manager.piece_manager.downloadBlocks(),
            self.peer_manager.piece_manager.totalBlocks,
            f"Kbps {self.peer_manager.findRate()} Peers {len(self.peer_manager.connected_peers)}",
            f"Completed {self.peer_manager.piece_manager.piecesDownloaded()}/{len(self.peer_manager.piece_manager.pieces)}"
        )

    def _finalize(self):
        self.tracker.stop_periodic_updates()
        self.peer_manager.exitPeerThreads()
        self.file_thread.join()
        
    async def monitor_block_timeouts(self, check_interval=5, timeout=10):
        while True:
            for piece in self.peer_manager.piece_manager.pieces:
                for block in piece.blocks:
                    if block.status == BlockStatus.REQUESTED and block.last_requested:
                        if time.time() - block.last_requested > timeout:
                            block.status = BlockStatus.EMPTY
                            block.last_requested = None
            await asyncio.sleep(check_interval)

    async def progress_printer(self):
        while not self.peer_manager.piece_manager.all_piece_complete():
            print(1)
            self.peer_manager.piece_manager.printProgressBar(
                self.peer_manager.piece_manager.downloadBlocks(),
                self.peer_manager.piece_manager.totalBlocks,
                f"Kbps {self.peer_manager.findRate()} Peers {len(self.peer_manager.connected_peers)}",
                f"{self.peer_manager.piece_manager.piecesDownloaded()}/{len(self.peer_manager.piece_manager.pieces)}",
                print_matrix=True,
                peers=self.peer_manager.peers
            )
            print(2)
            await asyncio.sleep(1)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python main.py <torrent_file> <download_path>")
        sys.exit(1)
    torrent = sys.argv[1]
    path = sys.argv[2]
    b = Bittorrent()
    b.start_downloading(torrent, path)