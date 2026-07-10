import importlib
import importlib.metadata

import remote_sandbox
from remote_sandbox.namespace import DEV_NAMESPACE


def test_package_version_uses_development_distribution(monkeypatch) -> None:
    requested_distributions: list[str] = []

    def fake_version(distribution_name: str) -> str:
        requested_distributions.append(distribution_name)
        return "1.2.3.dev0"

    try:
        with monkeypatch.context() as patch:
            patch.setattr(importlib.metadata, "version", fake_version)
            reported_version = importlib.reload(remote_sandbox).__version__
    finally:
        importlib.reload(remote_sandbox)

    assert requested_distributions == [DEV_NAMESPACE.distribution]
    assert reported_version == "1.2.3.dev0"
