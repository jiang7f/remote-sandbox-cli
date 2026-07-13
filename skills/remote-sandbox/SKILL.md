---
name: remote-sandbox
description: Operate remote-sandbox (`rsb`) workspaces as seamless local-code and remote-execution projects. Use when the user mentions rsb or remote-sandbox, invokes `$remote-sandbox`, names or asks about an rsb binding, says "use sfs", "用 sfs", or "管理 sfs", provides local and server directories to bind, asks to put a local project on a server, says "这个项目远程跑", or wants tests, builds, training, GPU work, or other commands run remotely while code remains locally editable.
---

# Remote Sandbox

Make rsb feel like one workspace. Keep file operations local and send only execution to the bound remote environment.

## Operating Model

- Treat the local tree as the canonical working tree. Read, search, create, and edit it with normal local tools.
- Treat the remote tree as the runtime. Run project commands there through rsb.
- Resolve the binding once, then work normally. Do not turn synchronization observation into a workflow.
- Do not inspect or edit project files through raw SSH. Do not use `scp`, `rsync`, manual archives, or remote temporary copies.
- Never delete a workspace root, forget a binding, or resolve a conflict without explicit user intent.

## Resolve the Workspace

Choose the first matching route.

### Current Local Project

When the current working directory is already inside the intended local project and the user asks for remote execution, use the current-directory fast path without a preliminary status call.

```bash
rsb run -- <command>
```

If rsb says the directory is not bound, then inspect `rsb status --paths` once and select the deepest `LOCAL` path containing the current directory.

### Named Binding

When the user names a binding such as `sfs`:

1. Run `rsb status <name> --paths` once.
2. Read `LOCAL`, `REMOTE`, and the current phase.
3. Use `LOCAL` as the working directory for all file tools.
4. Tell the user the selected name and paths in one concise sentence, then continue the requested task.

Do not reopen status unless execution fails or a generated remote file must be available locally.

### Exact Local and Remote Paths

When the user supplies both a local path and `target:/remote/path`:

1. Run `rsb status --paths` once and reuse an exact existing binding.
2. Otherwise derive a short ASCII name from the local directory.
3. Create the binding without opening a shell.

```bash
rsb connect <target> --remote <remote-path> --local <local-path> --name <name> --no-shell --yes
```

Check that the confirmation direction matches the user's request. Never force an unverified merge when both sides contain files.

### Put a Local Project on a Server

When the user supplies a local project and optionally a server, but no remote path:

1. Reuse a binding with the same `LOCAL` path if one exists.
2. Use the named server. If none is named, run `rsb list` once and choose a reachable server suited to the workload. Ask only when hardware or data locality makes the choice materially ambiguous.
3. Let rsb allocate an isolated path under `~/rsb-workspaces`.

```bash
rsb connect <target> --auto-remote --local <local-path> --name <name> --no-shell --yes
```

Report the generated binding and remote path, then continue the task. Do not browse the server to invent a project location.

## Execute Efficiently

Default to `rsb run`. It is non-interactive, preserves the remote command's output and exit status, supports long-running commands, and reuses rsb's SSH connection.

```bash
rsb run <name> -- pytest -q
rsb run <name> -- python3 train.py --epochs 10
```

When the tool can set its local working directory to the binding's `LOCAL` path, omit the name.

```bash
rsb run -- pytest -q
```

Use direct argument lists for ordinary commands. Batch dependent shell operations into one invocation only when shell semantics are required.

```bash
rsb run <name> -- bash -lc 'source .venv/bin/activate && pytest -q'
```

Do not run status before every command. Do not sleep merely to wait for synchronization. If a command appears to have seen an immediately preceding local edit too early, retry once after the current sync settles, then inspect status only if the problem remains.

Use one persistent `rsb shell <name>` only for genuinely interactive or stateful work such as a debugger, REPL, `top`, a foreground server, or an environment that must remain active. Do not open a shell merely because several ordinary commands are needed. If a shell is used, keep one terminal session open and return to `rsb run` for final verification.

## Handle Synchronization Only When It Matters

- `ready / idle` means proceed.
- `initial-syncing` means continue local inspection and editing, but wait before commands that require the complete remote tree.
- `stopped` means run `rsb start <name>` once.
- `disconnected` means run `rsb reconnect <name> --no-shell` in the foreground so authentication can recover.
- `degraded` with conflicts remains usable for unrelated paths. Run `rsb conflicts <name>` only when the task touches a conflicting path.
- `failed` means report the error and diagnose rsb. Do not bypass it with direct remote edits.

After `rsb run`, rsb requests synchronization of remote-generated project files. Continue normally unless the next step needs one of those files locally. In that case, check for the expected file first and inspect status once only if it is not present.

Never use `rsb status --watch` as background supervision for an AI task. Never repeatedly compare hashes. Keep remote command failure separate from a later synchronization warning.

## Interpret Short Requests

- "Use sfs and fix this" means resolve binding `sfs`, edit its local tree, and run verification remotely.
- "Run this project remotely" means use the current-directory fast path.
- "Bind local X to server:Y" means create or reuse an exact binding and continue working locally.
- "Put this project on server Y" means use `--auto-remote` and continue the requested task after binding.

Do not ask the user to restate rsb mechanics when one of these routes is clear.
