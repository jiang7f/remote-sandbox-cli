---
name: remote-sandbox
description: Use remote-sandbox (`rsb`) as one seamless workspace with local editing and remote execution. Trigger when the user names an rsb binding, asks for its local or remote directories, provides a local and remote directory to bind, asks to put a local project on a configured server, or wants code edited locally while tests, builds, training, or other commands run remotely.
---

# Remote Sandbox

Treat a binding as one workspace. Use the local tree as the normal file view and the remote tree as its execution environment. Let rsb synchronize in the background.

## Core Behavior

- Read, search, create, and edit files in the bound local tree with normal local tools.
- Run environment-dependent or compute-heavy commands in the bound remote root.
- Expect local edits to appear remotely and remote-generated project files to appear locally.
- Do not manually copy files with `ssh`, `scp`, `rsync`, or temporary staging directories.
- Do not make synchronization monitoring the main task. Inspect it only at the checkpoints below or when an operation actually fails.
- Never delete a workspace root. Never resolve a conflict without user intent.

## Route the User's Intent

### Existing Binding

When the user names a binding such as `sfs` or asks which directories it uses:

1. Run `rsb status <name> --paths` once.
2. Read the `LOCAL` and `REMOTE` columns.
3. State the selected binding and paths in one concise sentence.
4. Set the working directory to `LOCAL`.
5. Continue the requested coding task locally and use the remote execution strategy below.

Example response:

```text
Using sfs. Local: /local/project. Remote: server:/remote/project. I will edit locally and run project commands remotely.
```

Do not keep running `status` after this unless a command fails, remote output must sync back, or the task reaches a final synchronization checkpoint.

### Explicit Local and Remote Directories

When the user gives a local path and an exact remote endpoint such as `server:/remote/project`:

1. Use the requested SSH target, local path, and remote path.
2. Derive a short ASCII binding name from the local directory unless the user supplies one.
3. Check `rsb status --paths` once and reuse an existing exact binding.
4. Otherwise connect without entering an interactive shell.

```bash
rsb connect <target> \
  --remote <remote-path> \
  --local <local-path> \
  --name <name> \
  --no-shell
```

Review the confirmation text before accepting it. Accept local-to-remote when the user asked to upload local content. Accept remote-to-local when the user asked to open an existing remote project locally. Stop when both sides are nonempty and rsb refuses an unverified merge.

### Put a Local Project on a Server

When the user gives a local directory but no remote directory:

1. Reuse an existing binding whose `LOCAL` path matches.
2. If the user names a server, use it. Otherwise run `rsb list` and choose a reachable server suitable for the task. Prefer the least busy suitable target. Ask only when hardware requirements or data locality make the choice materially ambiguous.
3. Derive a short ASCII binding name from the local directory.
4. Let rsb allocate a safe, stable directory under `~/rsb-workspaces`.

```bash
rsb connect <target> \
  --auto-remote \
  --local <local-path> \
  --name <name> \
  --no-shell
```

Report the chosen target, generated remote path, and binding name, then continue the task. Do not browse the server or invent a path inside an existing project tree.

## Work Without Sync Chatter

After selecting or creating a binding:

- Treat `ready / idle` as normal and proceed.
- Treat `degraded` with conflicts as usable for unrelated paths. Inspect `rsb conflicts <name>` only when the task touches a conflicting path or the user asks.
- During `initial-syncing`, continue local reading and editing. Wait before running commands that require the complete remote tree.
- For `stopped`, run `rsb start <name>`.
- For `disconnected`, run the foreground reconnect suggested by `rsb status`.
- For `failed`, report the error and diagnose it. Do not bypass rsb with direct remote edits.
- Do not repeatedly compare hashes. rsb owns synchronization verification and automatically closes stale conflicts when both sides converge.

## Choose an Execution Mode

Use `rsb run` for one to three commands, fixed command batches, and final verification.

```bash
rsb run <name> -- pytest -q
rsb run <name> -- python3 train.py --epochs 10
rsb run <name> -- bash -lc 'make build && make test'
```

Use one persistent `rsb shell <name>` when the task requires many short, iterative commands and the terminal tool supports a persistent session. Keep that session open instead of reopening a shell for every command. Capture command status with a marker when needed.

```bash
<command>; __rsb_rc=$?; printf '\n__RSB_RC__=%s\n' "$__rsb_rc"
```

Use direct argument lists when possible. Use `bash -lc` only for shell syntax, environment activation, pipelines, or command chaining. Return to a standalone `rsb run` for the final test so its exit status is unambiguous.

Long-running commands may stay in `rsb run`. Do not move outputs to remote `/tmp` merely to avoid watcher activity.

## Finish

Run a final `rsb status <name>` only when remote commands generated project files, a sync warning occurred, or conflict state matters to the requested result. Otherwise report the remote command result directly.

Keep synchronization warnings separate from remote command exit status. Report the binding and paths once, then describe the task as ordinary local editing with remote execution.
