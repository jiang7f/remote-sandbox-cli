from __future__ import annotations

import subprocess
from dataclasses import dataclass

from remote_sandbox.ssh import validate_target

RESOURCE_COMMAND = (
    "echo '=CPU=' && (cat /proc/loadavg 2>/dev/null || uptime); "
    "echo '=NCPU=' && (nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1); "
    "echo '=MEM=' && (free -m 2>/dev/null | awk 'NR==2{print $2, $3, $4}' || "
    "vm_stat 2>/dev/null | awk '/Pages free/{free=$3} /Pages active/{active=$3} "
    "/Pages inactive/{inactive=$3} /Pages wired/{wired=$4} END{gsub(/\\./,\"\",free); "
    "gsub(/\\./,\"\",active); gsub(/\\./,\"\",inactive); gsub(/\\./,\"\",wired); "
    "used=(active+inactive+wired)*4096/1024/1024; "
    "total=(free+active+inactive+wired)*4096/1024/1024; "
    "printf \"%d %d %d\\n\", total, used, total-used}' || echo '0 0 0'); "
    "echo '=GPU=' && (nvidia-smi --query-gpu=index,utilization.gpu,memory.used,memory.total "
    "--format=csv,noheader,nounits 2>/dev/null || echo 'no_gpu')"
)


@dataclass(frozen=True)
class CpuStats:
    load_1m: float
    load_5m: float
    load_15m: float
    count: int


@dataclass(frozen=True)
class MemoryStats:
    total_mb: int
    used_mb: int
    free_mb: int
    used_pct: float


@dataclass(frozen=True)
class GpuStats:
    index: int
    util_pct: int
    mem_used_mb: int
    mem_total_mb: int
    mem_used_pct: float


@dataclass(frozen=True)
class ResourceStats:
    cpu: CpuStats
    memory: MemoryStats
    gpus: tuple[GpuStats, ...]
    idle_score: float


@dataclass(frozen=True)
class ProbeResult:
    target: str
    resources: ResourceStats | None
    error: str | None

    @classmethod
    def ok(cls, target: str, resources: ResourceStats) -> ProbeResult:
        return cls(target=target, resources=resources, error=None)

    @classmethod
    def failed(cls, target: str, error: str) -> ProbeResult:
        return cls(target=target, resources=None, error=error)


def probe_target_resources(target: str, *, timeout_s: float = 8.0) -> ProbeResult:
    try:
        safe_target = validate_target(target)
    except ValueError as exc:
        return ProbeResult.failed(target, str(exc))
    try:
        result = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", safe_target, RESOURCE_COMMAND],
            check=False,
            text=True,
            capture_output=True,
            timeout=timeout_s,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return ProbeResult.failed(safe_target, str(exc))
    if result.returncode not in (0, 1) and not result.stdout.strip():
        detail = result.stderr.strip() or f"exit {result.returncode}"
        return ProbeResult.failed(safe_target, detail)
    return ProbeResult.ok(safe_target, parse_resource_output(result.stdout))


def parse_resource_output(output: str) -> ResourceStats:
    sections = _split_sections(output)
    cpu = _parse_cpu(sections)
    memory = _parse_memory(sections)
    gpus = _parse_gpus(sections)
    idle_score = _idle_score(cpu, memory, gpus)
    return ResourceStats(cpu=cpu, memory=memory, gpus=gpus, idle_score=idle_score)


def format_resource_summary(result: ProbeResult) -> str:
    if result.error is not None or result.resources is None:
        return f"error: {result.error or 'resource probe failed'}"
    resources = result.resources
    gpu_summary = "no GPU"
    if resources.gpus:
        gpu_summary = "GPU " + " ".join(
            f"{gpu.index}:{gpu.util_pct}% {gpu.mem_used_mb}/{gpu.mem_total_mb}MB"
            for gpu in resources.gpus
        )
    return (
        f"idle {resources.idle_score:.2f} | "
        f"CPU {resources.cpu.load_1m:.2f}/{resources.cpu.count} | "
        f"MEM {resources.memory.used_pct:.1f}% | "
        f"{gpu_summary}"
    )


def _split_sections(output: str) -> dict[str, str]:
    sections: dict[str, str] = {}
    current_key: str | None = None
    buffer: list[str] = []
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("=") and stripped.endswith("=") and len(stripped) > 2:
            if current_key is not None:
                sections[current_key] = "\n".join(buffer).strip()
            current_key = stripped[1:-1]
            buffer = []
        elif current_key is not None:
            buffer.append(line)
    if current_key is not None:
        sections[current_key] = "\n".join(buffer).strip()
    return sections


def _parse_cpu(sections: dict[str, str]) -> CpuStats:
    loads = sections.get("CPU", "").split()
    try:
        load_1m = float(loads[0])
        load_5m = float(loads[1])
        load_15m = float(loads[2])
    except (IndexError, ValueError):
        load_1m = 0.0
        load_5m = 0.0
        load_15m = 0.0
    try:
        count = int(sections.get("NCPU", "1").strip())
    except ValueError:
        count = 1
    return CpuStats(load_1m=load_1m, load_5m=load_5m, load_15m=load_15m, count=max(count, 1))


def _parse_memory(sections: dict[str, str]) -> MemoryStats:
    parts = sections.get("MEM", "0 0 0").split()
    try:
        total_mb = int(float(parts[0]))
        used_mb = int(float(parts[1]))
        free_mb = int(float(parts[2]))
    except (IndexError, ValueError):
        total_mb = 0
        used_mb = 0
        free_mb = 0
    used_pct = round(used_mb / total_mb * 100, 1) if total_mb > 0 else 0.0
    return MemoryStats(
        total_mb=total_mb,
        used_mb=used_mb,
        free_mb=free_mb,
        used_pct=used_pct,
    )


def _parse_gpus(sections: dict[str, str]) -> tuple[GpuStats, ...]:
    gpu_text = sections.get("GPU", "no_gpu").strip()
    if not gpu_text or gpu_text == "no_gpu":
        return ()
    gpus: list[GpuStats] = []
    for line in gpu_text.splitlines():
        try:
            index_raw, util_raw, mem_used_raw, mem_total_raw = [
                part.strip() for part in line.split(",")
            ]
            mem_total = int(mem_total_raw)
            mem_used = int(mem_used_raw)
            gpus.append(
                GpuStats(
                    index=int(index_raw),
                    util_pct=int(util_raw),
                    mem_used_mb=mem_used,
                    mem_total_mb=mem_total,
                    mem_used_pct=round(mem_used / mem_total * 100, 1) if mem_total > 0 else 0.0,
                )
            )
        except (TypeError, ValueError):
            continue
    return tuple(gpus)


def _idle_score(cpu: CpuStats, memory: MemoryStats, gpus: tuple[GpuStats, ...]) -> float:
    cpu_ratio = min(cpu.load_1m / max(cpu.count, 1), 1.0)
    mem_ratio = memory.used_pct / 100.0
    gpu_ratio = sum(gpu.util_pct for gpu in gpus) / len(gpus) / 100.0 if gpus else 0.0
    return round((1.0 - cpu_ratio) * 0.4 + (1.0 - mem_ratio) * 0.4 + (1.0 - gpu_ratio) * 0.2, 3)
