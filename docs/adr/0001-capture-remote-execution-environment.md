# ADR-0001: Capture the remote execution environment

## Status

Accepted

## Context

SSH non-interactive commands commonly receive a smaller environment than an interactive shell.
Runtime managers, compiler paths, accelerator settings, and cluster modules may therefore be
available in `rsb shell` but absent from `rsb run`. Running every command through a persistent
interactive shell would introduce hidden state, require a TTY, and weaken exit-status and output
handling.

The execution model must remain manager-agnostic. It must also avoid copying exported secrets to
the local machine or retrying commands that may already have produced side effects.

## Decision

The remote agent captures exported environment variables from a controlled interactive Bash in
the workspace directory. It uses the same Bash initialization files as `rsb shell`.

The agent sanitizes shell-only and remote-sandbox internal variables, then writes a private shell
export file and a non-sensitive summary in the remote per-workspace metadata directory. Both files
use mode `0600` and remain on the remote machine.

`rsb run` asks the agent to ensure the snapshot exists and is current. The SSH command sources the
trusted export file and then executes the original argv without evaluating aliases or shell
functions. `rsb env show` returns only the summary. `rsb env refresh` forces recapture.

When capture fails, `rsb run` emits a warning and executes once with the SSH non-interactive
environment. It never retries the command under another environment. `--clean-env` intentionally
bypasses capture.

## Consequences

### Positive

- Normal commands see PATH and exported runtime settings users expect from `rsb shell`.
- Command arguments, output capture, exit status, and long-running behavior remain deterministic.
- The design is independent of Conda, venv, module systems, language managers, and accelerator
  toolchains.
- Complete environment values and possible secrets remain remote.
- Interactive-shell startup failures degrade explicitly instead of blocking all command execution.

### Negative

- The first command and explicit refresh incur one interactive-shell startup.
- Shell initialization with side effects can still fail or time out during capture.
- Aliases, functions, and manually activated state from an existing shell are not preserved.
- Exported environment values are persisted remotely until refresh or workspace cleanup.

### Neutral

- Changes to `/etc/bash.bashrc` or `~/.bashrc` invalidate the cached snapshot.
- Projects still need to document which runtime or command runner they require.

## Failure Modes

| Failure | Behavior |
|---|---|
| Bash is unavailable | Cache an unavailable state and run with the SSH environment |
| Shell initialization times out | Cache the warning and require explicit refresh to retry |
| Initialization output is noisy | Parse only data between unpredictable binary markers |
| Export file disappears after capture | Warn remotely and continue with the SSH environment |
| Export file is replaced by a symlink | Refuse to source it and return a command failure |
| Environment changes | Refresh automatically when tracked Bash initialization files change |

## Alternatives Considered

### Run every command in `rsb shell`

Rejected because it requires a TTY and introduces hidden state, prompt parsing, and weaker command
boundaries.

### Source `.bashrc` for every command

Rejected because startup files may be interactive-only, noisy, slow, or stateful. Repeated sourcing
also adds overhead and makes each command less predictable.

### Add manager-specific discovery

Rejected as the primary mechanism because it would require separate behavior for Conda, Mamba,
venv, modules, Pixi, Nix, language managers, and future tools.

### Keep the SSH non-interactive environment

Rejected as the default because it repeatedly surprises users and causes false conclusions that a
runtime is not installed. It remains available through `--clean-env`.
