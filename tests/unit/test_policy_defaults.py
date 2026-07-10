from remote_sandbox.policy import StaticPolicyEngine


def test_git_and_portability_caches_are_ignored_by_default() -> None:
    policy = StaticPolicyEngine()

    assert policy.is_ignored(".git/index")
    assert policy.is_ignored("nested/.git/config")
    assert policy.is_ignored(".venv/bin/python")
    assert policy.is_ignored("pkg/__pycache__/module.pyc")
    assert policy.is_ignored("node_modules/pkg/index.js")
    assert policy.is_ignored("pkg/.pytest_cache/v/cache/nodeids")
    assert policy.is_ignored("editor.swp")


def test_git_and_control_metadata_cannot_be_reenabled() -> None:
    policy = StaticPolicyEngine.from_lines(
        [
            "[sync]",
            ".git/**",
            "nested/.git/**",
            ".remote-sandbox/**",
            ".codex-remote-sandbox/**",
        ]
    )

    assert policy.is_ignored(".git/index")
    assert policy.is_ignored("nested/.git/config")
    assert policy.is_ignored(".remote-sandbox/state.sqlite3")
    assert policy.is_ignored(".codex-remote-sandbox/state.sqlite3")


def test_environment_cache_can_be_explicitly_reenabled() -> None:
    policy = StaticPolicyEngine.from_lines(
        [
            "[sync]",
            ".venv/**",
            "pkg/__pycache__/**",
        ]
    )

    assert not policy.is_ignored(".venv/bin/python")
    assert not policy.is_ignored("pkg/__pycache__/module.pyc")
    assert policy.is_ignored("node_modules/pkg/index.js")
