import threading

DEBUG_LOCK = threading.Lock()

def get_lock():
    return DEBUG_LOCK.acquire(False)

def free_lock():
    return DEBUG_LOCK.release()

