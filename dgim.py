"""DGIM algorithm (Datar-Gionis-Indyk-Motwani): approximate count of 1s in the last N bits of a stream
using O(log^2 N) memory instead of storing the whole window.

Rules: each bucket stores (size, end_timestamp); sizes are powers of 2; at most 2 buckets of each size;
when a 3rd bucket of one size appears, the two oldest merge into one of double size.
Estimate = sum of all bucket sizes, except only HALF of the oldest bucket (it may stick out of the window).
Error is at most 50%.
"""

class DGIM:
    def __init__(self, window):
        self.N = window
        self.t = 0
        self.buckets = []          # [size, end_timestamp], newest first

    def add(self, bit):
        self.t += 1
        while self.buckets and self.buckets[-1][1] <= self.t - self.N:   # drop buckets outside window
            self.buckets.pop()
        if bit:
            self.buckets.insert(0, [1, self.t])
            size = 1
            while True:
                idx = [i for i, b in enumerate(self.buckets) if b[0] == size]
                if len(idx) <= 2:
                    break
                newer, older = idx[-2], idx[-1]                          # two oldest of this size
                self.buckets[newer] = [size * 2, self.buckets[newer][1]]  # keep the more recent end time
                del self.buckets[older]
                size *= 2

    def count(self):
        if not self.buckets:
            return 0
        total = sum(b[0] for b in self.buckets[:-1])
        last = self.buckets[-1][0]
        return total + (1 if last == 1 else last // 2)