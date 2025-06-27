import sys

class Logger:
    RED = '\033[91m'
    GREEN = '\033[92m'
    END = '\033[0m'

    @staticmethod
    def error(msg, exc=None):
        if exc is not None:
            if isinstance(exc, ConnectionResetError) or ('10054' in str(exc)):
                print(f'{Logger.RED}[ERROR][Network] Peer disconnected (common for BitTorrent): {msg} — {exc}{Logger.END}', file=sys.stderr)
            else:
                print(f'{Logger.RED}[ERROR] {msg}: {exc}{Logger.END}', file=sys.stderr)
        else:
            print(f'{Logger.RED}[ERROR] {msg}{Logger.END}', file=sys.stderr)

    @staticmethod
    def info(msg):
        print(f'{Logger.GREEN}[INFO] {msg}{Logger.END}')

    @staticmethod
    def debug(msg):
        print(f'[DEBUG] {msg}')