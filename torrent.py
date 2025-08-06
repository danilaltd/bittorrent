from bcoding import bencode, bdecode
import os
import hashlib
import time

class Torrent:
    def __init__(self,torrent_path: str, download_path: str):
        self.decode_bencoded_file(torrent_path)
        self.initialize_files(download_path)
        self.request_peers_parameters()
    def decode_bencoded_file(self, torrent_path: str):
        with open(torrent_path, 'rb') as file:
            contents = bdecode(file)
        with open("torrent.txt", "w", encoding="utf-8") as file:
            for key, value in contents.items():
                file.write(f"{str(key)} : {str(value)}")
                file.write("\n\n")
        self.contents = contents
        self.announce_list = []
        if "announce-list" in self.contents:
            self.announce_list = [x[0] for x in self.contents["announce-list"]]
        else:
            self.announce_list = [self.contents["announce"]]
        # self.comment = self.contents["comment"]
        # self.creation_date = self.contents["created by"]
        #self.encoding = self.contents["encoding"]
        if "files" in self.contents["info"]:
            self._multipleFiles = True
            self.files = self.contents["info"]["files"]
        else:
            self._multipleFiles = False
            file_dictionary = {"length" : self.contents["info"]["length"], "path" : [self.contents["info"]["name"]]}
            self.files = [file_dictionary]
        self.name = self.contents["info"]["name"]
        self.piece_length = self.contents["info"]["piece length"]
        self.pieces = self.contents["info"]["pieces"]

    #init files
    def initialize_files(self, download_path: str):
        total_length = 0
        if self._multipleFiles == True:
            self.total_path = os.path.join(download_path, self.name)
            if not os.path.exists(self.total_path):
                os.mkdir(self.total_path, 0o777)
            for file in self.files:
                path_file = os.path.join(self.total_path, *file["path"])

                if not os.path.exists(os.path.dirname(path_file)):
                    os.makedirs(os.path.dirname(path_file))

                total_length += file["length"]
        else:
            self.total_path = download_path
            total_length = int(self.files[0]["length"])
        self.total_length = total_length
        
    def request_peers_parameters(self):
        self.info_hash = hashlib.sha1(bencode(self.contents["info"])).digest()
        self.peer_id = b'[\xfa\xc8U\xb9C\xcf\n\x0eg\xf4t\x06\xff\xb8|\x1b\xec2\xb4'
        # self.peer_id = self.generate_peer_id()
        self.left = self.total_length
        self.port = 6881

    def generate_peer_id(self) -> bytes:
        seed = str(time.time())
        return hashlib.sha1(seed.encode('utf-8')).digest()

