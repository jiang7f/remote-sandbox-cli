---
name: remote-sandbox
description: Use remote-sandbox (`rsb`) as one local-editing and remote-execution workspace. Activate only when the user explicitly mentions `rsb`, `remote-sandbox`, `远程沙盒`, or invokes `$remote-sandbox`. Never activate from generic SSH, server, GPU, remote execution, testing, training, build requests, or a possible binding name such as `sfs` alone. After explicit activation, keep the selected binding active for the rest of the conversation, including follow-ups such as `sfs`, `run`, or `远程跑`, until the user explicitly exits the remote sandbox or says not to use rsb.
---

# Remote Sandbox

Treat an explicitly selected rsb binding as one workspace. Keep project files local and use the remote side as its runtime.

## Maintain A Conversation-Level Mode

- Start with rsb disabled.
- Do not run any `rsb` command until the user explicitly says `rsb`, `remote-sandbox`, `远程沙盒`, or invokes `$remote-sandbox`.
- Do not activate rsb from a server name, a possible binding name, a GPU request, or generic phrases such as "run remotely" or "远程跑".
- After explicit activation and binding selection, keep rsb enabled for the rest of the conversation. Follow-ups such as "sfs", "run it", "跑一下", or "远程跑" use the selected binding by default.
- Disable rsb only when the user explicitly says to exit the remote sandbox, stop using rsb, or run locally. Do not carry the mode into a different conversation.

Before activation, these do not use rsb:

- "Use sfs and fix this."
- "Run the tests remotely."
- "Use the server GPU for training."

These activate rsb:

- "Use rsb to run the tests."
- "Use rsb binding sfs to reproduce the experiment."
- "用远程沙盒跑测试。"
- "Use $remote-sandbox for this project."

## Resolve And Announce Once

On the first rsb action in a conversation, resolve the binding before reading project files or running a remote command.

When the user names a binding such as `sfs`, run:

```bash
rsb status <name> --paths
```

When no binding is named:

1. Run `rsb status --paths` once.
2. Compare Codex's current working directory with each `LOCAL` path.
3. Select the deepest `LOCAL` path containing the current directory.
4. Ask which binding to use only when no current path matches and the choice is genuinely ambiguous.

Immediately announce the selected binding once:

```text
Using rsb binding sfs. Local: /local/project. Remote: server:/remote/project. I will edit locally and run commands remotely.
```

Do not ask the user to confirm an existing binding. Do not run status or repeat the path announcement on later turns. Resolve and announce again only when the user explicitly switches to another binding.

## Create A Binding When Explicitly Requested

For an exact local path and `target:/remote/path`, reuse an exact binding or create one without opening a shell:

```bash
rsb connect <target> --remote <remote-path> --local <local-path> --name <name> --no-shell --yes
```

When no remote path is given, reuse a matching local binding or allocate an isolated remote directory:

```bash
rsb connect <target> --auto-remote --local <local-path> --name <name> --no-shell --yes
```

The explicit binding request authorizes the normal one-way initialization. Stop if rsb refuses an unverified merge because both sides contain files. After creation, run `rsb status <name> --paths` and announce the paths once.

## Work As One Workspace

- Read, search, create, and edit only in the binding's local tree with normal local tools.
- Run environment-dependent commands in the remote root through rsb.
- Do not inspect or edit project files through raw SSH.
- Do not use `scp`, `rsync`, manual archives, or remote temporary copies to bypass rsb.
- Never delete a workspace root, forget a binding, or resolve a conflict without explicit user intent.

Default to `rsb run` for ordinary commands, final verification, and long-running jobs:

```bash
rsb run <name> -- pytest -q
rsb run <name> -- python3 train.py --epochs 10
```

After setting the terminal working directory to `LOCAL`, the name may be omitted:

```bash
rsb run -- pytest -q
```

Use one persistent `rsb shell <name>` only for interactive or stateful programs such as a debugger, REPL, `top`, or foreground server.

## Avoid Synchronization Chatter

- The one initial path lookup is sufficient. Do not run status before every command.
- Do not use `rsb status --watch` as background supervision for an AI task.
- Do not sleep merely to wait for synchronization or repeatedly compare hashes.
- Inspect status again only when execution fails, a required generated file has not appeared locally, or a conflict affects the task.
- Keep a remote command failure separate from a later synchronization warning.

Treat `ready / idle` as normal. For `stopped`, run `rsb start <name>`. For `disconnected`, use foreground `rsb reconnect <name> --no-shell`. For `degraded`, inspect only relevant conflicts. For `failed`, diagnose rsb instead of bypassing it.
