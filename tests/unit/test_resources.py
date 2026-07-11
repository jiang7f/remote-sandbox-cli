from __future__ import annotations

import subprocess

import pytest

import remote_sandbox.resources as resources


def test_resource_output_parses_cpu_memory_gpu_and_summary() -> None:
    result = resources.parse_resource_output(
        "=CPU=\n1.5 2.5 3.5 1/2 3\n"
        "=NCPU=\n8\n"
        "=MEM=\n16000 4000 12000\n"
        "=GPU=\n0, 25, 2048, 8192\n1, 75, 4096, 8192\n"
    )

    assert result.cpu == resources.CpuStats(1.5, 2.5, 3.5, 8)
    assert result.memory.used_pct == 25.0
    assert result.gpus[0].mem_used_pct == 25.0
    assert "GPU 0:25% 2048/8192MB" in resources.format_resource_summary(
        resources.ProbeResult.ok("gpu", result)
    )


def test_invalid_resource_fields_fall_back_without_crashing() -> None:
    result = resources.parse_resource_output(
        "ignored\n=CPU=\nbad\n=NCPU=\nbad\n=MEM=\nbad\n=GPU=\ninvalid\n0, 5, 1, 0\n"
    )

    assert result.cpu == resources.CpuStats(0.0, 0.0, 0.0, 1)
    assert result.memory == resources.MemoryStats(0, 0, 0, 0.0)
    assert result.gpus == (resources.GpuStats(0, 5, 1, 0, 0.0),)


def test_probe_rejects_target_and_classifies_subprocess_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert resources.probe_target_resources("-bad").error == "Invalid SSH target"

    def timeout(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        raise subprocess.TimeoutExpired("ssh", 1)

    monkeypatch.setattr(resources.subprocess, "run", timeout)
    assert "timed out" in (resources.probe_target_resources("host").error or "")


def test_probe_handles_failed_and_partial_ssh_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    failed = subprocess.CompletedProcess(["ssh"], 255, "", "network down")
    monkeypatch.setattr(resources.subprocess, "run", lambda *args, **kwargs: failed)
    assert resources.probe_target_resources("host").error == "network down"

    partial = subprocess.CompletedProcess(["ssh"], 2, "=NCPU=\n4\n", "warning")
    monkeypatch.setattr(resources.subprocess, "run", lambda *args, **kwargs: partial)
    result = resources.probe_target_resources("host")
    assert result.error is None
    assert result.resources is not None
    assert result.resources.cpu.count == 4


def test_error_summary_uses_explicit_or_default_message() -> None:
    assert resources.format_resource_summary(resources.ProbeResult.failed("host", "down")) == (
        "error: down"
    )
    assert resources.format_resource_summary(resources.ProbeResult("host", None, None)) == (
        "error: resource probe failed"
    )
