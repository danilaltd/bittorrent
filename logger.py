import sys
import threading
import time
import functools
from contextlib import contextmanager

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
    
    @staticmethod
    def lock_wait(lock_name, wait_time, thread_name=None):
        """Логирует время ожидания lock"""
        thread_info = f" (thread: {thread_name})" if thread_name else ""
        if wait_time > 5:  # Логируем только если ждали больше 100мс
            print(f'{Logger.BLUE}[LOCK] Waiting {wait_time:.3f}s for {lock_name}{thread_info}{Logger.END}')

# Глобальный словарь для отслеживания статистики locks
_lock_stats = {}
_lock_stats_lock = threading.Lock()

def get_lock_stats():
    """Возвращает статистику использования locks"""
    with _lock_stats_lock:
        return _lock_stats.copy()

def reset_lock_stats():
    """Сбрасывает статистику locks"""
    with _lock_stats_lock:
        _lock_stats.clear()

@contextmanager
def timed_lock(lock, lock_name, timeout=None):
    """
    Контекстный менеджер для логирования времени ожидания lock
    
    Args:
        lock: threading.Lock или threading.RLock
        lock_name: имя lock для логирования
        timeout: таймаут в секундах (None = без таймаута)
    """
    thread_name = threading.current_thread().name
    start_time = time.time()
    acquired = False
    
    try:
        if timeout is not None:
            acquired = lock.acquire(timeout=timeout)
            if not acquired:
                Logger.warning(f"Timeout waiting for {lock_name} (thread: {thread_name})")
                raise TimeoutError(f"Timeout waiting for {lock_name}")
        else:
            # print(f"{lock_name} in")
            lock.acquire()
            # print(f"{lock_name} out")
            acquired = True
        
        wait_time = time.time() - start_time
        Logger.lock_wait(lock_name, wait_time, thread_name)
        
        # Обновляем статистику
        with _lock_stats_lock:
            if lock_name not in _lock_stats:
                _lock_stats[lock_name] = {
                    'total_waits': 0,
                    'total_wait_time': 0.0,
                    'max_wait_time': 0.0,
                    'min_wait_time': float('inf'),
                    'current_holders': 0
                }
            stats = _lock_stats[lock_name]
            stats['total_waits'] += 1
            stats['total_wait_time'] += wait_time
            stats['max_wait_time'] = max(stats['max_wait_time'], wait_time)
            stats['min_wait_time'] = min(stats['min_wait_time'], wait_time)
            stats['current_holders'] += 1
        
        yield
        
    finally:
        # Обновляем статистику при освобождении только если блокировка была получена
        if acquired:
            with _lock_stats_lock:
                if lock_name in _lock_stats:
                    _lock_stats[lock_name]['current_holders'] -= 1
            
            lock.release()

def lock_decorator(lock_name, timeout=None):
    """
    Декоратор для методов, которые используют lock
    
    Args:
        lock_name: имя lock для логирования
        timeout: таймаут в секундах
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # Ищем lock в атрибутах объекта
            lock = None
            for attr_name in dir(self):
                attr = getattr(self, attr_name)
                # Проверяем, что это lock (используем строковое сравнение для избежания проблем с импортом)
                if hasattr(attr, 'acquire') and hasattr(attr, 'release'):
                    # Предполагаем, что lock имеет имя, содержащее lock_name
                    if lock_name.lower() in attr_name.lower():
                        lock = attr
                        break
            
            if lock is None:
                Logger.warning(f"Lock '{lock_name}' not found for method {func.__name__}")
                return func(self, *args, **kwargs)
            
            with timed_lock(lock, lock_name, timeout):
                return func(self, *args, **kwargs)
        
        return wrapper
    return decorator

def print_lock_stats():
    """Выводит статистику использования locks"""
    stats = get_lock_stats()
    if not stats:
        Logger.info("No lock statistics available")
        return
    
    Logger.info("=== Lock Statistics ===")
    for lock_name, lock_stats in stats.items():
        avg_wait = lock_stats['total_wait_time'] / lock_stats['total_waits'] if lock_stats['total_waits'] > 0 else 0
        min_wait = lock_stats['min_wait_time'] if lock_stats['min_wait_time'] != float('inf') else 0
        
        Logger.info(f"{lock_name}:")
        Logger.info(f"  Total waits: {lock_stats['total_waits']}")
        Logger.info(f"  Total wait time: {lock_stats['total_wait_time']:.3f}s")
        Logger.info(f"  Average wait time: {avg_wait:.3f}s")
        Logger.info(f"  Min wait time: {min_wait:.3f}s")
        Logger.info(f"  Max wait time: {lock_stats['max_wait_time']:.3f}s")
        Logger.info(f"  Current holders: {lock_stats['current_holders']}")
        Logger.info("")