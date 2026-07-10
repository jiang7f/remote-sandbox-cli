from collections.abc import Iterator
from pathlib import Path

import pytest
from helpers.sync_harness import SyncPair, make_sync_pair


@pytest.fixture
def sync_pair(tmp_path: Path) -> Iterator[SyncPair]:
    pair = make_sync_pair(tmp_path)
    yield pair
    pair.store.close()
    pair.remote_client.close()
