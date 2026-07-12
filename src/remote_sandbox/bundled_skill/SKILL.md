---
name: remote-sandbox
description: Use remote-sandbox (`rsb`) to discover or create SSH-backed workspace bindings, keep file edits local, and run project commands in the matching remote environment. Trigger whenever the user mentions rsb, remote-sandbox, a binding name, a local and remote workspace pair, or asks Codex to edit locally while testing, building, training, or executing on a remote server.
---

# Remote Sandbox

Use `rsb` as the ownership boundary between local editing and remote execution.

## Core Contract

- Read and edit source files only in the local working tree.
- Run commands that need the project environment or remote compute with `rsb run`.
- Use `rsb shell` only when the user explicitly needs an interactive terminal.
- Do not edit the remote project directly through `ssh`, `scp`, or remote shell commands.
- Do not delete either workspace root. `rsb forget` removes metadata, not project files.

## Discover the Binding

1. Run `rsb status --paths` before the first remote command.
2. Prefer the binding explicitly named by the user.
3. Otherwise select the binding whose `LOCAL` path contains the current working directory.
4. If no unique binding matches, ask for the binding name instead of guessing.
5. Run `rsb status <name> --paths` to capture the selected local and remote roots.

When the current directory is inside the bound local tree, commands may omit the name. Prefer the explicit name in reports and automation so the selected environment remains clear.

## Create a Binding

When the user provides an SSH target, remote path, and local path, use the noninteractive-shell form so Codex retains control of its terminal.

```bash
rsb connect <target> \
  --remote <remote-path> \
  --local <local-path> \
  --name <name> \
  --no-shell
```

Do not add `--yes` until the displayed local path, remote path, and initial sync direction have been checked. Do not force a merge when both directories are nonempty and `rsb` refuses a new binding.

If the user wants to browse before choosing a remote path, tell them to use `rsb enter <target>` in an interactive terminal. Do not automate an interactive browsing shell when an exact path is already available.

## Check Readiness

Inspect `rsb status <name> --paths` after connecting and before commands that require the complete workspace.

- `ready` means normal execution can proceed.
- `initial-syncing` or `syncing` means monitor with `rsb status <name>` until ready when the command needs the complete tree.
- `stopped` means run `rsb start <name>`, then check status again.
- `degraded`, `disconnected`, or `failed` means report the status error and use the suggested reconnect action. Do not bypass rsb with direct remote edits.
- A nonzero `CONFLICTS` count means inspect `rsb conflicts <name>`. Never choose local or remote conflict resolution without user intent or clear task evidence.

## Run Commands

Use argument-preserving remote execution.

```bash
rsb run <name> -- <command> <args...>
```

Examples:

```bash
rsb run project -- pytest -q
rsb run project -- python3 train.py --epochs 10
rsb run project -- bash -lc 'make build && make test'
```

Prefer direct argument lists. Use `bash -lc` only when shell syntax such as pipes, redirection, environment activation, or command chaining is required.

Treat the remote command exit status as the command result. Report a synchronization warning separately rather than describing a successful remote command as failed.

## Finish the Task

Run `rsb status <name>` after commands that create or modify files. Before reporting completion, confirm that the remote command result is known and that the binding is not in `failed` or unresolved conflict state.
