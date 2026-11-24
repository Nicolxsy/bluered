"""Pure-Python deterministic Bloom filter with normalized filename tokenization."""

from __future__ import annotations

import base64
import hashlib
import math
import re
import unicodedata

_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def tokenize_filename(name: str) -> list[str]:
    """Normalize accents/case and split a filename or query into unique ordered tokens."""
    normalized = unicodedata.normalize("NFKD", name)
    ascii_text = "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()
    seen: set[str] = set()
    tokens: list[str] = []
    for part in _NON_ALNUM_RE.split(ascii_text):
        if part and part not in seen:
            seen.add(part)
            tokens.append(part)
    return tokens


class BloomFilter:
    """Deterministic Bloom filter using SHA-256 and BLAKE2b double hashing."""

    def __init__(
        self,
        size_bits: int = 8192,
        hash_functions_k: int = 5,
        bitset: bytes | bytearray | None = None,
    ) -> None:
        if size_bits <= 0:
            raise ValueError("size_bits must be positive")
        if hash_functions_k < 1:
            raise ValueError("hash_functions_k must be >= 1")
        expected_len = (size_bits + 7) // 8
        if bitset is None:
            self._bits = bytearray(expected_len)
        else:
            if len(bitset) != expected_len:
                raise ValueError(
                    f"bitset length {len(bitset)} does not match expected {expected_len} bytes"
                )
            self._bits = bytearray(bitset)
        self._size_bits = size_bits
        self._hash_functions_k = hash_functions_k

    @property
    def size_bits(self) -> int:
        return self._size_bits

    @property
    def hash_functions_k(self) -> int:
        return self._hash_functions_k

    def _bit_indices(self, token: str) -> tuple[int, ...]:
        data = token.encode("utf-8")
        h1 = int.from_bytes(hashlib.sha256(data).digest()[:8], "big")
        h2 = int.from_bytes(hashlib.blake2b(data, digest_size=8).digest(), "big") | 1
        return tuple((h1 + i * h2) % self._size_bits for i in range(self._hash_functions_k))

    def _set_raw_token(self, token: str) -> None:
        for idx in self._bit_indices(token):
            self._bits[idx // 8] |= 1 << (idx % 8)

    def _has_raw_token(self, token: str) -> bool:
        return all(
            bool(self._bits[idx // 8] & (1 << (idx % 8))) for idx in self._bit_indices(token)
        )

    def add_token(self, token: str) -> None:
        """Normalize and insert a single token (or sub-tokens if composite)."""
        for part in tokenize_filename(token):
            self._set_raw_token(part)

    def add_filename(self, filename: str) -> None:
        """Tokenize and insert all normalized tokens from a filename."""
        for token in tokenize_filename(filename):
            self._set_raw_token(token)

    def contains_token(self, token: str) -> bool:
        """Return True if all normalized parts of token may be present."""
        parts = tokenize_filename(token)
        if not parts:
            return False
        return all(self._has_raw_token(part) for part in parts)

    def might_match_query(self, query: str) -> bool:
        """Return True if every normalized token in query may be present in the filter."""
        tokens = tokenize_filename(query)
        if not tokens:
            return False
        return all(self._has_raw_token(token) for token in tokens)

    def to_bytes(self) -> bytes:
        return bytes(self._bits)

    @classmethod
    def from_bytes(
        cls,
        data: bytes,
        *,
        size_bits: int,
        hash_functions_k: int,
    ) -> BloomFilter:
        return cls(size_bits=size_bits, hash_functions_k=hash_functions_k, bitset=data)

    def to_base64(self) -> str:
        return base64.b64encode(self._bits).decode("ascii")

    @classmethod
    def from_base64(
        cls,
        encoded: str,
        *,
        size_bits: int,
        hash_functions_k: int,
    ) -> BloomFilter:
        raw = base64.b64decode(encoded.encode("ascii"), validate=True)
        return cls(size_bits=size_bits, hash_functions_k=hash_functions_k, bitset=raw)

    @classmethod
    def optimal(cls, expected_items: int, false_positive_rate: float = 0.01) -> BloomFilter:
        """Create a BloomFilter sized for expected_items and target false_positive_rate."""
        if expected_items <= 0:
            raise ValueError("expected_items must be positive")
        if not (0.0 < false_positive_rate < 1.0):
            raise ValueError("false_positive_rate must be between 0 and 1")
        raw_bits = math.ceil(
            -expected_items * math.log(false_positive_rate) / (math.log(2) ** 2)
        )
        size_bits = max(8, ((raw_bits + 7) // 8) * 8)
        hash_functions_k = max(1, round((size_bits / expected_items) * math.log(2)))
        return cls(size_bits=size_bits, hash_functions_k=hash_functions_k)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BloomFilter):
            return NotImplemented
        return (
            self._size_bits == other._size_bits
            and self._hash_functions_k == other._hash_functions_k
            and self._bits == other._bits
        )
