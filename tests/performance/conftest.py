from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from helpers.sync_harness import PerformancePair, make_performance_pair


@pytest.fixture
def performance_pair(tmp_path: Path) -> Iterator[PerformancePair]:
    pair = make_performance_pair(tmp_path)
    try:
        yield pair
    finally:
        pair.close()
