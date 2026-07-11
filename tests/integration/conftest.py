from collections.abc import Iterator
from pathlib import Path

import pytest
from helpers.sync_harness import (
    CliHarness,
    DaemonPairHarness,
    FakePtyBackendHarness,
    InitialPairHarness,
    PromptShellHarness,
    SyncPair,
    make_cli_harness,
    make_daemon_pair,
    make_initial_pair,
    make_prompt_shell_harness,
    make_sync_pair,
)


@pytest.fixture
def cli_fixture(tmp_path: Path) -> Iterator[CliHarness]:
    harness = make_cli_harness(tmp_path)
    yield harness
    harness.store.close()
    harness.pair.remote_client.close()


@pytest.fixture
def fake_pty_backend() -> FakePtyBackendHarness:
    return FakePtyBackendHarness()


@pytest.fixture
def shell_fixture() -> PromptShellHarness:
    return make_prompt_shell_harness()


@pytest.fixture
def daemon_pair(tmp_path: Path) -> Iterator[DaemonPairHarness]:
    harness = make_daemon_pair(tmp_path)
    try:
        yield harness
    finally:
        harness.close()


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
