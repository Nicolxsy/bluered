"""Unit tests for shared/bluered_contracts/bloom.py."""

from __future__ import annotations

import pytest
from bluered_contracts.bloom import BloomFilter, tokenize_filename


def test_tokenize_filename_normalizes_accents_case_and_delimiters() -> None:
    assert tokenize_filename("Canción_Número-01.MP3") == ["cancion", "numero", "01", "mp3"]
    assert tokenize_filename("  Álgebra--Lineal_2026_ALGEBRA.pdf ") == [
        "algebra",
        "lineal",
        "2026",
        "pdf",
    ]
    assert tokenize_filename("   ---   ") == []


def test_bloom_determinism_and_zero_false_negatives() -> None:
    bf1 = BloomFilter(size_bits=4096, hash_functions_k=5)
    bf2 = BloomFilter(size_bits=4096, hash_functions_k=5)

    filenames = [f"Track_{i:03d}_Estudio_Clásico.flac" for i in range(100)]
    for name in filenames:
        bf1.add_filename(name)
        bf2.add_filename(name)

    assert bf1 == bf2
    assert bf1.to_bytes() == bf2.to_bytes()

    for name in filenames:
        assert bf1.might_match_query(name)
        assert bf1.contains_token("clasico")

    assert not bf1.might_match_query("inexistente_totalmente_xyz")
    assert not bf1.might_match_query("   ")
    assert not bf1.contains_token("")


def test_bloom_serialization_bytes_and_base64() -> None:
    bf = BloomFilter.optimal(expected_items=50, false_positive_rate=0.01)
    bf.add_token("Sistemas Distribuidos")

    raw = bf.to_bytes()
    b64 = bf.to_base64()

    restored_bytes = BloomFilter.from_bytes(
        raw,
        size_bits=bf.size_bits,
        hash_functions_k=bf.hash_functions_k,
    )
    restored_b64 = BloomFilter.from_base64(
        b64,
        size_bits=bf.size_bits,
        hash_functions_k=bf.hash_functions_k,
    )

    assert restored_bytes == bf
    assert restored_b64 == bf
    assert restored_b64.might_match_query("distribuidos")


def test_bloom_validation_errors() -> None:
    with pytest.raises(ValueError):
        BloomFilter(size_bits=0, hash_functions_k=3)
    with pytest.raises(ValueError):
        BloomFilter(size_bits=64, hash_functions_k=0)
    with pytest.raises(ValueError):
        BloomFilter.from_bytes(b"\x00", size_bits=64, hash_functions_k=3)
    with pytest.raises(ValueError):
        BloomFilter.optimal(expected_items=0)
    with pytest.raises(ValueError):
        BloomFilter.optimal(expected_items=10, false_positive_rate=1.0)
