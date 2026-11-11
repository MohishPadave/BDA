"""Bloom filter: space-efficient set membership. May give false positives, never false negatives.
Used here as the 'known faulty sensors' list. Items cannot be removed.
m = -n*ln(p)/(ln2)^2 bits and k = (m/n)*ln2 hash functions for n expected items and false-positive rate p.
"""
import hashlib, math

class BloomFilter:
    def __init__(self, capacity=1000, error_rate=0.01):
        self.m = max(8, int(-capacity * math.log(error_rate) / (math.log(2) ** 2)))
        self.k = max(1, round(self.m / capacity * math.log(2)))
        self.bits = bytearray((self.m + 7) // 8)
        self.count = 0

    def _positions(self, item):
        d = hashlib.sha256(item.encode()).digest()
        h1, h2 = int.from_bytes(d[:8], "big"), int.from_bytes(d[8:16], "big") | 1
        return [(h1 + i * h2) % self.m for i in range(self.k)]   # double hashing

    def add(self, item):
        if item in self:
            return
        for p in self._positions(item):
            self.bits[p // 8] |= 1 << (p % 8)
        self.count += 1

    def __contains__(self, item):
        return all(self.bits[p // 8] & (1 << (p % 8)) for p in self._positions(item))

    def fp_rate(self):
        """Current estimated false-positive probability: (1 - e^(-k*n/m))^k"""
        return (1 - math.exp(-self.k * self.count / self.m)) ** self.k