from __future__ import annotations

import os

import numpy as np

import ghtrader.distributed as distu


def test_is_rank0_default_true(monkeypatch):
    monkeypatch.delenv("RANK", raising=False)
    assert distu.is_rank0() is True


def test_world_size_default_one(monkeypatch):
    monkeypatch.delenv("WORLD_SIZE", raising=False)
    assert distu.get_world_size() == 1
    assert distu.is_distributed() is False


def test_seed_everything_deterministic_cpu():
    distu.seed_everything(123)
    a = np.random.randn(5)
    distu.seed_everything(123)
    b = np.random.randn(5)
    assert np.allclose(a, b)

