from __future__ import annotations

from pathlib import Path

from helpers.sync_harness import CliHarness, DaemonPairHarness

from remote_sandbox.manifest import EntryFingerprint, MissingEntry, fingerprint_local


def test_use_local_resolution_transfers_selected_version_and_closes_conflict(
    cli_fixture: CliHarness,
) -> None:
    conflict = cli_fixture.create_conflict(
        path="model.py",
        base=b"base\n",
        local=b"local\n",
        remote=b"remote\n",
    )

    result = cli_fixture.run(["resolve", "model.py", "--use-local"])

    assert result.exit_code == 0
    assert cli_fixture.remote_bytes("model.py") == b"local\n"
    assert cli_fixture.store.get_conflict(conflict.conflict_id).resolved_at is not None
    assert cli_fixture.store.get_expected_echo("remote", "model.py") is not None


def test_conflicts_lists_only_unresolved_paths(cli_fixture: CliHarness) -> None:
    conflict = cli_fixture.create_conflict(
        path="model.py",
        base=b"base\n",
        local=b"local\n",
        remote=b"remote\n",
    )
    cli_fixture.store.create_conflict(
        path="resolved.py",
        reason="both-modified",
        local_blob=b"local\n",
        remote_blob=b"remote\n",
    )
    resolved = cli_fixture.store.list_conflicts()[-1]
    cli_fixture.store.resolve_conflict(resolved.conflict_id)

    result = cli_fixture.run(["conflicts", "dq"])

    assert result.exit_code == 0
    assert conflict.path in result.stdout
    assert "resolved.py" not in result.stdout


def test_use_remote_resolution_transfers_selected_version_and_closes_conflict(
    cli_fixture: CliHarness,
) -> None:
    conflict = cli_fixture.create_conflict(
        path="model.py",
        base=b"base\n",
        local=b"local\n",
        remote=b"remote\n",
    )

    result = cli_fixture.run(["resolve", "model.py", "--use-remote"])

    assert result.exit_code == 0
    assert cli_fixture.local_bytes("model.py") == b"remote\n"
    assert cli_fixture.store.get_conflict(conflict.conflict_id).resolved_at is not None
    assert cli_fixture.store.get_expected_echo("local", "model.py") is not None


def test_resolve_use_local_can_select_deletion(cli_fixture: CliHarness) -> None:
    path = "deleted-by-local.txt"
    for root in (cli_fixture.pair.local, cli_fixture.pair.remote):
        (root / path).write_bytes(b"base")
    cli_fixture.pair.seed_current_base()
    (cli_fixture.pair.local / path).unlink()
    (cli_fixture.pair.remote / path).write_bytes(b"remote changed")
    remote = fingerprint_local(cli_fixture.pair.remote, path, with_hash=True)
    conflict = cli_fixture.store.create_conflict(
        path=path,
        reason="delete-versus-modify",
        local_blob=None,
        remote_blob=b"remote changed",
        local_fingerprint=None,
        remote_fingerprint=remote,
    )

    result = cli_fixture.run(["resolve", path, "--use-local"])

    assert result.exit_code == 0
    assert not (cli_fixture.pair.local / path).exists()
    assert not (cli_fixture.pair.remote / path).exists()
    assert path not in cli_fixture.store.list_base()
    assert cli_fixture.store.get_expected_echo("remote", path) == MissingEntry(path)
    assert cli_fixture.store.get_conflict(conflict.conflict_id).resolved_at is not None


def test_resolve_use_remote_can_select_deletion(cli_fixture: CliHarness) -> None:
    path = "deleted-by-remote.txt"
    for root in (cli_fixture.pair.local, cli_fixture.pair.remote):
        (root / path).write_bytes(b"base")
    cli_fixture.pair.seed_current_base()
    (cli_fixture.pair.local / path).write_bytes(b"local changed")
    (cli_fixture.pair.remote / path).unlink()
    local = fingerprint_local(cli_fixture.pair.local, path, with_hash=True)
    conflict = cli_fixture.store.create_conflict(
        path=path,
        reason="modify-versus-delete",
        local_blob=b"local changed",
        remote_blob=None,
        local_fingerprint=local,
        remote_fingerprint=None,
    )

    result = cli_fixture.run(["resolve", path, "--use-remote"])

    assert result.exit_code == 0
    assert not (cli_fixture.pair.local / path).exists()
    assert not (cli_fixture.pair.remote / path).exists()
    assert path not in cli_fixture.store.list_base()
    assert cli_fixture.store.get_expected_echo("local", path) == MissingEntry(path)
    assert cli_fixture.store.get_conflict(conflict.conflict_id).resolved_at is not None


def test_deletion_winner_rejects_selected_side_reappearance(
    cli_fixture: CliHarness,
) -> None:
    path = "reappeared.txt"
    for root in (cli_fixture.pair.local, cli_fixture.pair.remote):
        (root / path).write_bytes(b"base")
    cli_fixture.pair.seed_current_base()
    (cli_fixture.pair.local / path).unlink()
    remote_before = b"remote changed"
    (cli_fixture.pair.remote / path).write_bytes(remote_before)
    remote = fingerprint_local(cli_fixture.pair.remote, path, with_hash=True)
    conflict = cli_fixture.store.create_conflict(
        path=path,
        reason="delete-versus-modify",
        local_blob=None,
        remote_blob=remote_before,
        local_fingerprint=None,
        remote_fingerprint=remote,
    )
    (cli_fixture.pair.local / path).write_bytes(b"reappeared")

    result = cli_fixture.run(["resolve", path, "--use-local"])

    assert result.exit_code == 2
    assert "selected source changed" in result.stderr
    assert (cli_fixture.pair.remote / path).read_bytes() == remote_before
    assert path in cli_fixture.store.list_base()
    assert cli_fixture.store.get_conflict(conflict.conflict_id).resolved_at is None


def test_deletion_failure_rolls_back_conflict_base_and_echo(
    cli_fixture: CliHarness,
) -> None:
    path = "delete-fails.txt"
    for root in (cli_fixture.pair.local, cli_fixture.pair.remote):
        (root / path).write_bytes(b"base")
    cli_fixture.pair.seed_current_base()
    base_before = cli_fixture.store.list_base()[path]
    (cli_fixture.pair.local / path).unlink()
    (cli_fixture.pair.remote / path).write_bytes(b"remote changed")
    remote = fingerprint_local(cli_fixture.pair.remote, path, with_hash=True)
    conflict = cli_fixture.store.create_conflict(
        path=path,
        reason="delete-versus-modify",
        local_blob=None,
        remote_blob=b"remote changed",
        local_fingerprint=None,
        remote_fingerprint=remote,
    )
    cli_fixture.pair.transport.change_destination_before_delete(
        "remote",
        path,
        b"changed before delete",
    )

    result = cli_fixture.run(["resolve", path, "--use-local"])

    assert result.exit_code == 2
    assert "destination changed" in result.stderr
    assert cli_fixture.store.list_base()[path] == base_before
    assert cli_fixture.store.get_expected_echo("remote", path) is None
    assert cli_fixture.store.get_conflict(conflict.conflict_id).resolved_at is None


def test_resolve_rejects_path_escape(cli_fixture: CliHarness) -> None:
    result = cli_fixture.run(["resolve", "../outside", "--use-local"])

    assert result.exit_code == 2
    assert "Invalid relative path" in result.stderr


def test_resolve_rejects_symlink_parent(cli_fixture: CliHarness, tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (cli_fixture.pair.local / "linked").symlink_to(outside, target_is_directory=True)

    result = cli_fixture.run(["resolve", "linked/model.py", "--use-local"])

    assert result.exit_code == 2
    assert "symlink parent" in result.stderr


def test_resolve_does_not_overwrite_when_selected_source_changes(
    cli_fixture: CliHarness,
) -> None:
    conflict = cli_fixture.create_conflict(
        path="model.py",
        base=b"base\n",
        local=b"local\n",
        remote=b"remote\n",
    )
    cli_fixture.change_selected_source_before_transfer("model.py")

    result = cli_fixture.run(["resolve", "model.py", "--use-local"])

    assert result.exit_code == 2
    assert "selected source changed" in result.stderr
    assert cli_fixture.remote_bytes("model.py") == b"remote\n"
    assert cli_fixture.store.get_conflict(conflict.conflict_id).resolved_at is None


def test_running_supervisor_resolves_without_pid_or_watcher_restart(
    daemon_pair: DaemonPairHarness,
) -> None:
    daemon_pair.wait_until_ready()
    before = daemon_pair.client.control_status()
    before_order = list(daemon_pair.restart_order)
    for root, content in (
        (daemon_pair.local, b"local\n"),
        (daemon_pair.remote, b"remote\n"),
    ):
        (root / "model.py").write_bytes(content)
    local = fingerprint_local(daemon_pair.local, "model.py", with_hash=True)
    remote = fingerprint_local(daemon_pair.remote, "model.py", with_hash=True)
    assert isinstance(local, EntryFingerprint)
    assert isinstance(remote, EntryFingerprint)
    conflict = daemon_pair.store.create_conflict(
        path="model.py",
        reason="both-modified",
        local_blob=b"local\n",
        remote_blob=b"remote\n",
        local_fingerprint=local,
        remote_fingerprint=remote,
    )

    result = daemon_pair.client.mutate(
        "resolve",
        {"path": "model.py", "use_local": True},
    )

    after = daemon_pair.client.control_status()
    assert result["path"] == "model.py"
    assert before.pid == after.pid
    after_order = daemon_pair.restart_order
    for label in ("local-watcher", "remote-watcher", "subscription"):
        assert after_order.count(label) == before_order.count(label)
    assert after_order[-1] == "engine:event"
    assert (daemon_pair.remote / "model.py").read_bytes() == b"local\n"
    assert daemon_pair.store.get_conflict(conflict.conflict_id).resolved_at is not None
