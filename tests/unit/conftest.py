from collections.abc import Iterator
from pathlib import Path

import pytest
from helpers.sync_harness import EngineHarness, make_engine_harness


@pytest.fixture
def engine_fixture(tmp_path: Path) -> Iterator[EngineHarness]:
    harness = make_engine_harness(tmp_path)
    yield harness
    harness.store.close()
    harness.remote_client.close()
