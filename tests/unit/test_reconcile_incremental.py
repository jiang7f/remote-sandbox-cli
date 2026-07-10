from collections.abc import Iterable
from dataclasses import FrozenInstanceError

import pytest

import remote_sandbox.reconcile as reconcile
from remote_sandbox.manifest import EntryFingerprint, EntryKind, MissingEntry
from remote_sandbox.policy import StaticPolicyEngine
from remote_sandbox.reconcile import (
    ActionType,
    HashRequest,
    SyncPlan,
    build_incremental_plan,
)


def file(path: str, digest: str | None) -> EntryFingerprint:
    return EntryFingerprint(path, EntryKind.FILE, 4, 1, 0o100644, content_hash=digest)


def quick_file(
    path: str,
    *,
    size: int = 4,
    mtime_ns: int = 1,
    mode: int = 0o100644,
    digest: str | None = None,
) -> EntryFingerprint:
    return EntryFingerprint(path, EntryKind.FILE, size, mtime_ns, mode, content_hash=digest)


def symlink(path: str, target: str, digest: str | None = None) -> EntryFingerprint:
    return EntryFingerprint(
        path,
        EntryKind.SYMLINK,
        None,
        1,
        0o120777,
        link_target=target,
        content_hash=digest,
    )


def test_only_local_change_pushes_remote() -> None:
    plan = build_incremental_plan(
        base={"a.py": file("a.py", "old")},
        local={"a.py": file("a.py", "new")},
        remote={"a.py": file("a.py", "old")},
        dirty_paths={"a.py"},
        policy=StaticPolicyEngine(),
    )
    assert [action.type for action in plan.actions] == [ActionType.PUSH]


def test_ambiguous_changed_file_requests_hash_before_decision() -> None:
    plan = build_incremental_plan(
        base={"a.py": file("a.py", "old")},
        local={"a.py": quick_file("a.py", mtime_ns=2)},
        remote={"a.py": file("a.py", "old")},
        dirty_paths={"a.py"},
        policy=StaticPolicyEngine(),
    )
    assert [(item.side, item.path) for item in plan.hash_requests] == [("local", "a.py")]


def test_local_quick_change_requests_only_local_hash_when_remote_quick_matches_base() -> None:
    plan = build_incremental_plan(
        base={"a.py": quick_file("a.py", digest="base")},
        local={"a.py": quick_file("a.py", mtime_ns=2)},
        remote={"a.py": quick_file("a.py")},
        dirty_paths={"a.py"},
        policy=StaticPolicyEngine(),
    )

    assert [(request.side, request.path) for request in plan.hash_requests] == [
        ("local", "a.py")
    ]
    assert plan.actions == ()
    assert plan.conflicts == ()


def test_remote_quick_change_requests_only_remote_hash_when_local_quick_matches_base() -> None:
    plan = build_incremental_plan(
        base={"a.py": quick_file("a.py", digest="base")},
        local={"a.py": quick_file("a.py")},
        remote={"a.py": quick_file("a.py", mtime_ns=2)},
        dirty_paths={"a.py"},
        policy=StaticPolicyEngine(),
    )

    assert [(request.side, request.path) for request in plan.hash_requests] == [
        ("remote", "a.py")
    ]
    assert plan.actions == ()
    assert plan.conflicts == ()


def test_both_quick_fingerprints_matching_base_need_no_hash_or_plan_output() -> None:
    plan = build_incremental_plan(
        base={"a.py": quick_file("a.py", digest="base")},
        local={"a.py": quick_file("a.py")},
        remote={"a.py": quick_file("a.py")},
        dirty_paths={"a.py"},
        policy=StaticPolicyEngine(),
    )

    assert plan == SyncPlan()


def test_both_quick_changes_request_both_hashes_in_side_order() -> None:
    plan = build_incremental_plan(
        base={"a.py": quick_file("a.py", digest="base")},
        local={"a.py": quick_file("a.py", mtime_ns=2)},
        remote={"a.py": quick_file("a.py", size=5)},
        dirty_paths={"a.py"},
        policy=StaticPolicyEngine(),
    )

    assert [(request.side, request.path) for request in plan.hash_requests] == [
        ("local", "a.py"),
        ("remote", "a.py"),
    ]
    assert plan.actions == ()
    assert plan.conflicts == ()


def test_strong_local_change_does_not_request_unchanged_remote_quick_hash() -> None:
    plan = build_incremental_plan(
        base={"a.py": quick_file("a.py", digest="base")},
        local={"a.py": quick_file("a.py", mtime_ns=2, digest="local")},
        remote={"a.py": quick_file("a.py")},
        dirty_paths={"a.py"},
        policy=StaticPolicyEngine(),
    )

    assert plan.hash_requests == ()
    assert [action.type for action in plan.actions] == [ActionType.PUSH]
    assert plan.conflicts == ()


def test_both_sides_reaching_the_same_content_updates_only_the_base() -> None:
    plan = build_incremental_plan(
        base={"a.py": file("a.py", "old")},
        local={"a.py": file("a.py", "same")},
        remote={"a.py": file("a.py", "same")},
        dirty_paths={"a.py"},
        policy=StaticPolicyEngine(),
    )
    assert [action.type for action in plan.actions] == [ActionType.UPDATE_BASE]
    assert plan.conflicts == ()


def test_only_dirty_non_ignored_paths_are_examined_or_emitted() -> None:
    class GuardedEntries(dict[str, EntryFingerprint | MissingEntry]):
        def get(  # type: ignore[override]
            self,
            key: str,
            default: object = None,
        ) -> EntryFingerprint | MissingEntry | object:
            if key not in {"dirty.py"}:
                raise AssertionError(f"planner examined {key}")
            return super().get(key, default)

    base = GuardedEntries(
        {
            "dirty.py": file("dirty.py", "old"),
            "clean.py": file("clean.py", "old"),
        }
    )
    local = GuardedEntries(
        {
            "dirty.py": file("dirty.py", "new"),
            "clean.py": file("clean.py", "new"),
        }
    )
    remote = GuardedEntries(
        {
            "dirty.py": file("dirty.py", "old"),
            "clean.py": file("clean.py", "old"),
        }
    )

    plan = build_incremental_plan(
        base=base,
        local=local,
        remote=remote,
        dirty_paths=[".git/index", "dirty.py"],
        policy=StaticPolicyEngine(),
    )

    assert [(action.type, action.path) for action in plan.actions] == [
        (ActionType.PUSH, "dirty.py")
    ]
    assert plan.hash_requests == ()
    assert plan.conflicts == ()
    assert plan.warnings == ()


@pytest.mark.parametrize(
    ("base", "local", "remote", "expected"),
    [
        ({}, {"a.py": file("a.py", "local")}, {}, ActionType.PUSH),
        ({}, {}, {"a.py": file("a.py", "remote")}, ActionType.PULL),
        (
            {"a.py": file("a.py", "old")},
            {"a.py": file("a.py", "local")},
            {"a.py": file("a.py", "old")},
            ActionType.PUSH,
        ),
        (
            {"a.py": file("a.py", "old")},
            {"a.py": file("a.py", "old")},
            {"a.py": file("a.py", "remote")},
            ActionType.PULL,
        ),
        (
            {"a.py": file("a.py", "old")},
            {"a.py": MissingEntry("a.py")},
            {"a.py": file("a.py", "old")},
            ActionType.DELETE_REMOTE,
        ),
        (
            {"a.py": file("a.py", "old")},
            {"a.py": file("a.py", "old")},
            {"a.py": MissingEntry("a.py")},
            ActionType.DELETE_LOCAL,
        ),
    ],
)
def test_one_sided_create_modify_and_delete_propagate_in_both_directions(
    base: dict[str, EntryFingerprint | MissingEntry],
    local: dict[str, EntryFingerprint | MissingEntry],
    remote: dict[str, EntryFingerprint | MissingEntry],
    expected: ActionType,
) -> None:
    plan = build_incremental_plan(
        base=base,
        local=local,
        remote=remote,
        dirty_paths={"a.py"},
        policy=StaticPolicyEngine(),
    )

    assert [action.type for action in plan.actions] == [expected]
    assert plan.hash_requests == ()
    assert plan.conflicts == ()


def test_hash_requests_are_minimal_and_deterministic_per_side() -> None:
    plan = build_incremental_plan(
        base={path: file(path, "old") for path in ("a.py", "b.py", "c.py")},
        local={
            "a.py": quick_file("a.py", mtime_ns=2),
            "b.py": file("b.py", "new"),
            "c.py": quick_file("c.py", mtime_ns=2),
        },
        remote={
            "a.py": file("a.py", "old"),
            "b.py": quick_file("b.py", mtime_ns=2),
            "c.py": quick_file("c.py", size=5),
        },
        dirty_paths=["c.py", "b.py", "a.py", "c.py"],
        policy=StaticPolicyEngine(),
    )

    assert [(request.side, request.path) for request in plan.hash_requests] == [
        ("local", "a.py"),
        ("remote", "b.py"),
        ("local", "c.py"),
        ("remote", "c.py"),
    ]
    assert plan.actions == ()
    assert plan.conflicts == ()


def test_symlink_identity_uses_target_text_instead_of_digest() -> None:
    plan = build_incremental_plan(
        base={"current": symlink("current", "old", "base-digest")},
        local={"current": symlink("current", "release", "local-digest")},
        remote={"current": symlink("current", "release", "remote-digest")},
        dirty_paths={"current"},
        policy=StaticPolicyEngine(),
    )

    assert [action.type for action in plan.actions] == [ActionType.UPDATE_BASE]
    assert plan.hash_requests == ()


def test_different_symlink_targets_conflict_even_when_digests_match() -> None:
    plan = build_incremental_plan(
        base={"current": symlink("current", "old", "digest")},
        local={"current": symlink("current", "local", "digest")},
        remote={"current": symlink("current", "remote", "digest")},
        dirty_paths={"current"},
        policy=StaticPolicyEngine(),
    )

    assert plan.actions == ()
    assert [conflict.reason for conflict in plan.conflicts] == ["both-modified"]


def test_dirty_paths_are_normalized_deduplicated_and_sorted() -> None:
    plan = build_incremental_plan(
        base={},
        local={
            "a.py": file("a.py", "a"),
            "b.py": file("b.py", "b"),
        },
        remote={},
        dirty_paths=["b.py", "pkg/../a.py", "a.py", "b.py"],
        policy=StaticPolicyEngine(),
    )

    assert [(action.type, action.path) for action in plan.actions] == [
        (ActionType.PUSH, "a.py"),
        (ActionType.PUSH, "b.py"),
    ]


def test_entries_absent_from_all_input_maps_are_cleanly_ignored() -> None:
    plan = build_incremental_plan(
        base={},
        local={},
        remote={},
        dirty_paths={"gone.py"},
        policy=StaticPolicyEngine(),
    )

    assert plan == SyncPlan()


@pytest.mark.parametrize(
    "dirty_paths",
    ["a.py", ["../escape"], ["/absolute"], [1], None],
)
def test_invalid_dirty_path_inputs_are_rejected(dirty_paths: object) -> None:
    with pytest.raises(ValueError):
        build_incremental_plan(
            base={},
            local={},
            remote={},
            dirty_paths=dirty_paths,  # type: ignore[arg-type]
            policy=StaticPolicyEngine(),
        )


def test_dirty_entry_types_and_paths_are_validated_exactly() -> None:
    class FingerprintSubclass(EntryFingerprint):
        pass

    invalid_entries: Iterable[object] = (
        object(),
        file("other.py", "hash"),
        MissingEntry(),
        FingerprintSubclass("a.py", EntryKind.FILE, 4, 1, 0o100644, content_hash="hash"),
    )
    for entry in invalid_entries:
        with pytest.raises(ValueError):
            build_incremental_plan(
                base={},
                local={"a.py": entry},  # type: ignore[dict-item]
                remote={},
                dirty_paths={"a.py"},
                policy=StaticPolicyEngine(),
            )


def test_public_plan_models_are_strict_and_immutable() -> None:
    assert {member.name for member in ActionType} == {
        "PUSH",
        "PULL",
        "DELETE_LOCAL",
        "DELETE_REMOTE",
        "UPDATE_BASE",
    }
    request = HashRequest("local", "pkg/../a.py")
    assert request.path == "a.py"
    with pytest.raises(FrozenInstanceError):
        request.path = "changed.py"  # type: ignore[misc]
    with pytest.raises(ValueError):
        HashRequest("base", "a.py")
    with pytest.raises(ValueError):
        SyncPlan(actions=[])  # type: ignore[arg-type]
    assert not hasattr(reconcile, "PlanActionType")
    assert not hasattr(reconcile, "PlanAction")
