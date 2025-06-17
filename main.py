from socket import socket
from Messages import keep_alive
from peer import Peer
from torrent import Torrent
from tracker import Tracker
from PeerManager import PeerManager
from BlockandPiece import Piece, BLOCK_SIZE
import socket
import random
import threading
import time
import os, sys

class Bittorrent:
    def __init__(self) -> None:
        pass
    def startDownloading(self, torrent, path):
        print_matrix = False
        tracker = Tracker(torrent, path)
        tracker.get_peer_list()
        time.sleep(7)
        tracker.exitAllThreads()

        p = PeerManager(tracker)
        print(tracker.torrent_obj.total_length)
        p.connect()
        time.sleep(5)

        # Start periodic tracker updates
        tracker.start_periodic_updates()

        minHeap = p.piece_manager.getRarestPieceMinHeap(p.connected_peers)
        file_thread = threading.Thread(target=p.piece_manager.write_into_file)
        file_thread.start()

        torrent_completed = False
        while p.piece_manager.all_piece_complete() == False:
            # Update tracker with current stats
            downloaded = p.piece_manager.downloadBlocks() * BLOCK_SIZE
            uploaded = sum(peer.uploaded for peer in p.connected_peers)
            tracker.update_stats(downloaded, uploaded)
            
            # Update optimistic unchoke
            p.update_optimistic_unchoke()
            
            # First try to download from pre-selected pieces
            if p.pre_selected_pieces:
                for index in p.pre_selected_pieces:
                    piece = p.piece_manager.pieces[index]
                    if piece.is_complete():
                        continue
                        
                    peers = p.get_peer_having_piece(piece)
                    if not peers:
                        continue
                    
                    peer = p.get_best_peer(peers)
                    if not peer or peer.peer_choking:
                        continue
                    
                    blocks_to_request = min(16, len(piece.blocks))
                    for block_index in range(blocks_to_request):
                        if piece.blocks[block_index].status == 0:
                            try:
                                request_block = p.request_blockByteString(piece, block_index, piece.blocks[block_index])
                                if time.time() - peer.last_transmission > 60:
                                    request_block += keep_alive.byteStringForKeepAlive()
                                    peer.last_transmission = time.time()
                                peer.sock.send(request_block)
                                peer.last_transmission = time.time()
                                piece.blocks[block_index].last_requested = time.time()
                                
                                p.prefetch_next_blocks(peer.sock, piece, block_index, peer)
                                
                            except Exception as e:
                                if peer in p.connected_peers:
                                    p.connected_peers.remove(peer)
                                print(f"{e} for {peer.ip_port}")
                                time.sleep(1)
                                break
            
            # If no pre-selected pieces or they're all complete, use rarest first strategy
            minHeap = p.piece_manager.getRarestPieceMinHeap(p.connected_peers)
            for x in minHeap:
                index = x[0]
                piece = p.piece_manager.pieces[index]
                if piece.is_complete():
                    continue
                    
                peers = p.get_peer_having_piece(piece)
                if not peers:                    
                    print("No Peers.. Connecting to peer again...")
                    time.sleep(10)
                    continue
                
                peer = p.get_best_peer(peers)
                if not peer or peer.peer_choking:
                    continue
                
                blocks_to_request = min(16, len(piece.blocks))
                for block_index in range(blocks_to_request):
                    if piece.blocks[block_index].status == 0:
                        try:
                            request_block = p.request_blockByteString(piece, block_index, piece.blocks[block_index])
                            if time.time() - peer.last_transmission > 60:
                                request_block += keep_alive.byteStringForKeepAlive()
                                peer.last_transmission = time.time()
                            peer.sock.send(request_block)
                            peer.last_transmission = time.time()
                            piece.blocks[block_index].last_requested = time.time()
                            
                            p.prefetch_next_blocks(peer.sock, piece, block_index, peer)
                            
                        except Exception as e:
                            if peer in p.connected_peers:
                                p.connected_peers.remove(peer)
                            print(f"{e} for {peer.ip_port}")
                            time.sleep(1)
                            break
                
                p.piece_manager.printProgressBar(p.piece_manager.downloadBlocks(), p.piece_manager.totalBlocks, 
                    f"Kbps {p.findRate()} Peers {len(p.connected_peers)}", 
                    f"Completed {p.piece_manager.piecesDownloaded()}/{len(p.piece_manager.pieces)}", print_matrix=print_matrix)
                
        print("Torrent Complete")
        print(tracker.torrent_obj.total_length)
        torrent_completed = True
        p.torrent_completed = True

        # Send completed event to trackers
        for url in tracker.torrent_obj.announce_list:
            if "http" in url:
                threading.Thread(target=tracker.http_request, args=(url, 'completed')).start()
            elif "udp" in url:
                threading.Thread(target=tracker.udp_request, args=(url, 'completed')).start()

        p.piece_manager.printProgressBar(p.piece_manager.downloadBlocks(), p.piece_manager.totalBlocks, 
                    f"Kbps {p.findRate()} Peers {len(p.connected_peers)}", 
                    f"Completed {p.piece_manager.piecesDownloaded()}/{len(p.piece_manager.pieces)}", print_matrix=print_matrix)
        
        # Stop tracker updates and wait for threads
        tracker.stop_periodic_updates()
        p.exitPeerThreads()
        file_thread.join()

        
torrent = sys.argv[1]
path = sys.argv[2]
b = Bittorrent()
b.startDownloading(torrent, path)