from test_reconcile_incremental import file

from remote_sandbox.manifest import EntryFingerprint, EntryKind, MissingEntry
from remote_sandbox.policy import StaticPolicyEngine
from remote_sandbox.reconcile import build_incremental_plan


def directory(path: str) -> EntryFingerprint:
    return EntryFingerprint(path, EntryKind.DIR, None, 1, 0o040755)


def test_both_modified_preserves_a_conflict_instead_of_selecting_a_winner() -> None:
    plan = build_incremental_plan(
        base={"model.py": file("model.py", "base")},
        local={"model.py": file("model.py", "local")},
        remote={"model.py": file("model.py", "remote")},
        dirty_paths={"model.py"},
        policy=StaticPolicyEngine(),
    )
    assert not plan.actions
    assert plan.conflicts[0].path == "model.py"
    assert plan.conflicts[0].reason == "both-modified"


def test_deleted_placeholder_conflicts_with_unchanged_remote_source() -> None:
    placeholder = EntryFingerprint(
        "weights.bin",
        EntryKind.FILE,
        50_000_000,
        1,
        0o100644,
        content_hash="remote",
        is_placeholder=True,
    )
    remote = file("weights.bin", "remote")
    plan = build_incremental_plan(
        base={"weights.bin": placeholder},
        local={"weights.bin": MissingEntry("weights.bin")},
        remote={"weights.bin": remote},
        dirty_paths={"weights.bin"},
        policy=StaticPolicyEngine(),
    )
    assert plan.actions == ()
    assert plan.conflicts[0].reason == "placeholder-changed"


def test_special_entry_warns_without_blocking_unrelated_paths() -> None:
    special = EntryFingerprint("socket", EntryKind.SPECIAL, None, 1, 0o140777)
    plan = build_incremental_plan(
        base={},
        local={"notes.py": file("notes.py", "new"), "socket": special},
        remote={"notes.py": MissingEntry("notes.py"), "socket": MissingEntry("socket")},
        dirty_paths={"socket", "notes.py"},
        policy=StaticPolicyEngine(),
    )
    assert [(action.type.value, action.path) for action in plan.actions] == [
        ("push", "notes.py")
    ]
    assert [(warning.path, warning.reason) for warning in plan.warnings] == [
        ("socket", "special-entry-not-transferred")
    ]


def test_kind_divergence_is_an_explicit_non_destructive_conflict() -> None:
    plan = build_incremental_plan(
        base={"entry": file("entry", "old")},
        local={"entry": directory("entry")},
        remote={"entry": file("entry", "old")},
        dirty_paths={"entry"},
        policy=StaticPolicyEngine(),
    )

    assert plan.actions == ()
    assert plan.hash_requests == ()
    assert [(conflict.path, conflict.reason) for conflict in plan.conflicts] == [
        ("entry", "kind-divergence")
    ]


def test_kind_divergence_does_not_request_an_unneeded_file_hash() -> None:
    plan = build_incremental_plan(
        base={"entry": file("entry", "old")},
        local={"entry": directory("entry")},
        remote={"entry": file("entry", None)},
        dirty_paths={"entry"},
        policy=StaticPolicyEngine(),
    )

    assert plan.hash_requests == ()
    assert [conflict.reason for conflict in plan.conflicts] == ["kind-divergence"]


def test_delete_versus_modify_conflicts_in_both_directions() -> None:
    base = file("model.py", "base")
    for local, remote in (
        (MissingEntry("model.py"), file("model.py", "remote")),
        (file("model.py", "local"), MissingEntry("model.py")),
    ):
        plan = build_incremental_plan(
            base={"model.py": base},
            local={"model.py": local},
            remote={"model.py": remote},
            dirty_paths={"model.py"},
            policy=StaticPolicyEngine(),
        )

        assert plan.actions == ()
        assert [conflict.reason for conflict in plan.conflicts] == ["delete-versus-modify"]


def test_regular_file_replacing_a_placeholder_is_a_conflict() -> None:
    placeholder = EntryFingerprint(
        "weights.bin",
        EntryKind.FILE,
        50_000_000,
        1,
        0o100644,
        content_hash="remote",
        is_placeholder=True,
    )
    plan = build_incremental_plan(
        base={"weights.bin": placeholder},
        local={"weights.bin": file("weights.bin", "local")},
        remote={"weights.bin": file("weights.bin", "remote")},
        dirty_paths={"weights.bin"},
        policy=StaticPolicyEngine(),
    )

    assert plan.actions == ()
    assert [conflict.reason for conflict in plan.conflicts] == ["placeholder-changed"]


def test_modified_placeholder_metadata_is_a_conflict() -> None:
    placeholder = EntryFingerprint(
        "weights.bin",
        EntryKind.FILE,
        50_000_000,
        1,
        0o100644,
        content_hash="remote",
        is_placeholder=True,
    )
    edited = EntryFingerprint(
        "weights.bin",
        EntryKind.FILE,
        49_999_999,
        1,
        0o100644,
        content_hash="remote",
        is_placeholder=True,
    )
    plan = build_incremental_plan(
        base={"weights.bin": placeholder},
        local={"weights.bin": edited},
        remote={"weights.bin": file("weights.bin", "remote")},
        dirty_paths={"weights.bin"},
        policy=StaticPolicyEngine(),
    )

    assert plan.actions == ()
    assert [conflict.reason for conflict in plan.conflicts] == ["placeholder-changed"]


def test_unchanged_placeholder_can_follow_a_remote_update() -> None:
    placeholder = EntryFingerprint(
        "weights.bin",
        EntryKind.FILE,
        50_000_000,
        1,
        0o100644,
        content_hash="old",
        is_placeholder=True,
    )
    plan = build_incremental_plan(
        base={"weights.bin": placeholder},
        local={"weights.bin": placeholder},
        remote={"weights.bin": file("weights.bin", "new")},
        dirty_paths={"weights.bin"},
        policy=StaticPolicyEngine(),
    )

    assert [action.type.value for action in plan.actions] == ["pull"]
    assert plan.conflicts == ()


def test_matching_deletions_only_update_the_base() -> None:
    plan = build_incremental_plan(
        base={"gone.py": file("gone.py", "old")},
        local={"gone.py": MissingEntry("gone.py")},
        remote={"gone.py": MissingEntry("gone.py")},
        dirty_paths={"gone.py"},
        policy=StaticPolicyEngine(),
    )

    assert [action.type.value for action in plan.actions] == ["update-base"]
    assert isinstance(plan.actions[0].base_after, MissingEntry)
    assert plan.conflicts == ()
