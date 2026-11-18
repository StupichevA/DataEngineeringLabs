import os
import time
import resource


def _cgroup_peak_mb():
    for p in ("/sys/fs/cgroup/memory.peak", "/sys/fs/cgroup/memory/memory.max_usage_in_bytes"):
        try:
            with open(p) as f:
                return int(f.read().strip()) / 1024 / 1024
        except Exception:
            continue
    return None


class Watch:
    def __init__(self):
        self.t0 = time.time()
        self.seconds = 0.0
        self.peak_mb = 0.0
        self.cgroup_peak_mb = None

    def stop(self):
        self.seconds = time.time() - self.t0
        own = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        child = resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss
        self.peak_mb = max(own, child) / 1024  # Linux: КБ -> МБ
        self.cgroup_peak_mb = _cgroup_peak_mb()