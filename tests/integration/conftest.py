from collections.abc import Iterator
from pathlib import Path

import pytest
from helpers.sync_harness import (
    InitialPairHarness,
    SyncPair,
    make_initial_pair,
    make_sync_pair,
)


@pytest.fixture
def sync_pair(tmp_path: Path) -> Iterator[SyncPair]:
    pair = make_sync_pair(tmp_path)
    yield pair
    pair.store.close()
    pair.remote_client.close()


@pytest.fixture
def initial_pair(tmp_path: Path) -> Iterator[InitialPairHarness]:
    pair = make_initial_pair(tmp_path)
    try:
        yield pair
    finally:
        pair.close()
