from collections.abc import Iterator
from pathlib import Path

import pytest
from helpers.sync_harness import (
    EngineHarness,
    SupervisorHarness,
    make_engine_harness,
    make_supervisor_harness,
)


@pytest.fixture
def engine_fixture(tmp_path: Path) -> Iterator[EngineHarness]:
    harness = make_engine_harness(tmp_path)
    yield harness
    harness.store.close()
    harness.remote_client.close()


@pytest.fixture
def supervisor_fixture(tmp_path: Path) -> Iterator[SupervisorHarness]:
    harness = make_supervisor_harness(tmp_path)
    yield harness
    harness.close()
