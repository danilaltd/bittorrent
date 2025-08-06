import sys
import os
from datetime import datetime

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
        path = 'logs'
    else:
        name = 'locks.log'
        path = 'logs'
        
    with open(os.path.join(f'{path}', f"{name}"), 'a', encoding='utf-8') as f:
        f.write(res)

class Logger:
    RED = '\033[91m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
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
    
    @staticmethod
    def warning(msg):
        print(f'{Logger.YELLOW}[WARNING] {msg}{Logger.END}')