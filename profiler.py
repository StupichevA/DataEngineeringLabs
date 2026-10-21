import time
import psutil
import threading
import os


class Watch:
    def __init__(self):
        self.me = psutil.Process(os.getpid())
        self.peak = 0
        self.running = True
        self.start_time = time.perf_counter()
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def _loop(self):
        while self.running:
            try:
                total_mem = self.me.memory_info().rss
                for child in self.me.children(recursive=True):
                    try:
                        total_mem += child.memory_info().rss
                    except psutil.NoSuchProcess:
                        pass

                if total_mem > self.peak:
                    self.peak = total_mem
            except psutil.NoSuchProcess:
                break
            time.sleep(0.05)

    def stop(self):
        self.running = False
        self.thread.join()
        self.seconds = time.perf_counter() - self.start_time
        self.peak_mb = self.peak / (1024 * 1024)
        return self