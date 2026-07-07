# remote-sandbox

`remote-sandbox` 是一个与编辑器无关的远程工作区 CLI：在本地编辑代码，让 AI 只读写本地目录，同时把命令实际运行在远程服务器上，并由后台 daemon 自动同步两边文件。

适合这样的工作流：本地 Claude Code / Codex / 编辑器负责读写项目，GPU、大内存、大数据相关命令留在远程机器执行。

## 安装

用 `uv` 安装为本机工具：

```bash
uv tool install remote-sandbox
```

也可以用 `pipx`：

```bash
pipx install remote-sandbox
```

安装后会得到两个等价命令：

```bash
rsb --help
remote-sandbox --help
```

## 前置条件

- 本机需要有 OpenSSH 客户端，也就是 `ssh` 命令可用。
- 远程机器需要能通过 OpenSSH 登录，并且有可用的 `python3`。
- `rsb enter` 需要远程机器有 `bash`，因为它会临时注入一个只在当前 shell 生效的 `rsb connect` 函数。
- 服务器、用户名、端口、私钥、跳板机等连接信息建议写在 `~/.ssh/config`。

## 快速开始

```bash
# 1. 查看 SSH 配置里的可用服务器
rsb list

# 2. 把当前本地目录绑定到远程目录；--no-shell 表示只启动同步 daemon
rsb connect <target> -r '~/workspaces/project' --name train --no-shell

# 3. 在远程工作区运行一次性命令
rsb run train -- python -V

# 4. 需要交互时打开包壳远程 shell
rsb shell train
```

如果你还不知道远程目录在哪里，可以先进入远程机器浏览：

```bash
rsb enter <target>
# 在远程 shell 里 cd 到目标目录后执行：
rsb connect --name train
```

## 命令概览

```bash
rsb list
rsb status
rsb set placeholder-limit 10MB
rsb enter <target> [-r/--remote <remote-path>] [-l/--local <local-path>]
rsb connect [-r/--remote <remote-path>] [-l/--local <local-path>] [--name <name>]     # 在 rsb enter 里面使用
rsb connect <target> -r/--remote <remote-path> [-l/--local <local-path>] [--name <name>] [--no-shell]
rsb reconnect <name> [-l/--local <local-path>] [--no-shell]
rsb forget <name>
rsb start [name]
rsb stop [name]
rsb shell [name]
rsb run [name] -- <command> [args...]
rsb peek <path> [--lines N | --tail N]
rsb fetch <path>
rsb fetch -- <path>
rsb fetch -a/--all
```

也可以使用完整命令名：

```bash
remote-sandbox <command>
```

## 服务器来源

`rsb` 默认读取 OpenSSH 配置，不额外维护服务器配置文件。

```text
~/.ssh/config
```

`<target>` 可以是 `~/.ssh/config` 里的 `Host` 别名，也可以是 OpenSSH 支持的目标格式，例如：

```bash
rsb enter <target>
rsb connect user@example.com --remote '~/workspaces/project'
```

`ProxyJump`、`IdentityFile`、端口、用户名等连接细节都交给 OpenSSH 处理。

## 查看服务器

列出 SSH 配置中的服务器，并并发探测 CPU、内存和 GPU 状态：

```bash
rsb list
```

输出格式：

```text
placeholder-limit: 10MB

TARGET    BOUND  CONNECTIONS  CPU      MEM    GPU
server-a  yes    train        0.42/32  18.5%  0:12% 2048/24576MB
server-b  -      -            1.10/64  33.2%  none
```

字段含义：

- `TARGET`：SSH 目标名。
- `BOUND`：是否已有本地目录绑定到该服务器。
- `CONNECTIONS`：该服务器下已经记录的连接名。
- `CPU`：1 分钟负载 / CPU 核心数。
- `MEM`：内存使用率。
- `GPU`：GPU 使用率和显存占用；没有 GPU 时显示 `none`。

## 查看绑定

列出当前机器上已经绑定的工作区：

```bash
rsb status
```

输出格式：

```text
NAME   REMOTE    LOCAL                REMOTE_PATH           DAEMON         CURRENT
train  server-a  /local/path/project  ~/workspaces/project  running:12345  *
eval   server-b  /local/path/other    /data/other           stopped
```

`DAEMON` 显示该绑定工作区的同步 daemon 是否正在运行；`CURRENT` 为 `*` 表示当前目录属于该绑定工作区。

## 用户级设置

`rsb` 会把用户级设置和连接记录放在：

```text
~/.remote-sandbox/config.toml
~/.remote-sandbox/connections.toml
```

默认大文件占位阈值是 `10MB`。超过这个大小的远程文件默认不会直接拉到本地，而是在本地写入占位文件。

修改默认阈值：

```bash
rsb set placeholder-limit 100MB
```

这个设置是用户级的，不写在项目 `.rsbignore` 里。

## 进入服务器后绑定

如果你还不知道远程目录在哪里，先进入服务器：

```bash
rsb enter <target>
```

进入后，你看到的是远程 shell，可以正常执行 `pwd`、`ls`、`cd` 等命令来找目录。找到要绑定的目录后，在这个 shell 里执行：

```bash
rsb connect
```

默认会绑定：

- 远程目录：当前远程 shell 所在目录。
- 本地目录：你执行 `rsb enter <target>` 时所在的本地目录。

也可以显式指定远程目录、本地目录和连接名：

```bash
rsb connect -r /data/project
rsb connect -l /local/path/project
rsb connect -r /data/project -l /local/path/project
rsb connect --name train
```

`--name` 是这个绑定的本机连接名。后续可以用它来重连、启停 daemon、打开 shell 或运行命令，例如 `rsb reconnect train`、`rsb run train -- python -V`。没有指定 `--name` 时，`rsb` 会生成一个可读名字，例如 `server-a-project`；如果重名，会追加 `-2`。

这个命令会退出临时浏览 shell，将本地目录与选中的远程目录绑定，执行首次同步，启动本地同步 daemon，然后进入绑定后的远程工作区 shell。

## 直接绑定

如果你已经知道远程目录，可以跳过 `rsb enter`，直接绑定：

```bash
rsb connect <target> -r '~/workspaces/project'
```

默认把当前本地目录作为本地工作区。也可以显式指定本地目录：

```bash
rsb connect <target> -r /data/project -l /local/path/project --name train
```

绑定成功后会记录一个连接名，并启动本地同步 daemon。默认还会进入一个包壳远程 shell；如果只想启动 daemon、不进入 shell，可以加 `--no-shell`：

```bash
rsb connect <target> -r /data/project --no-shell
```

之后可以按名字重连，或显式启停 daemon：

```bash
rsb reconnect train
rsb start train
rsb stop train
```

如果记录里的本地目录已经不存在，`reconnect` 会报错，不会偷偷重新创建旧目录。你可以指定新的本地目录来修复记录：

```bash
rsb reconnect train --local /new/local/project --no-shell
```

不再需要某个连接名时，可以删除本机连接记录：

```bash
rsb forget train
```

`forget` 只删除本机连接记录，不删除本地或远程文件。

## 远程执行

需要多开 shell 时，可以直接打开新的包壳远程 shell；一次性命令用 `rsb run`：

```bash
rsb shell train
rsb run train -- python -V
```

包壳 shell 和 `rsb run` 都不自己做同步；远程命令结束后会通知本地 daemon 立即同步一次。你在 shell 里执行的命令实际运行在远程目录中，例如：

```bash
python train.py
nvidia-smi
bash scripts/run.sh
```

## 绑定规则

首次绑定时，`rsb` 会在本地和远程创建匹配的工作区标记：

```text
<local>/.remote-sandbox/workspace.toml
<remote>/.remote-sandbox/workspace.toml
```

标记中包含相同的 `workspace_id`。后续连接时，`rsb` 会用它确认本地目录和远程目录是否属于同一个工作区。

当前安全规则：

- 远程路径必须是 `/absolute`、`~` 或 `~/path`，并且不能是远程根目录 `/`。
- SSH 目标不能为空，不能以 `-` 开头，不能包含控制字符。
- 如果远程目录非空且没有绑定标记，终端会要求确认。
- 如果本地和远程的 `workspace_id` 不一致，连接会拒绝继续。
- 绑定成功后，会启动本地同步 daemon；默认进入对应远程目录的包壳 shell，`--no-shell` 则只启动 daemon。

## 同步模型

当前实现使用 SQLite 保存 base 快照，并基于 `base`、`local`、`remote` 三方 manifest 做判断。文件新旧不依赖 mtime 猜测，而是基于内容哈希。

同步策略支持三种模式：

- `sync`：正常双向同步。
- `placeholder`：远程大文件在本地保留占位文本，需要时再拉取。
- `ignore`：该路径不由 remote-sandbox 管理。

本地同步 daemon 使用事件驱动 watcher 观察本地变更并上传。包壳 shell 或 `rsb run` 中的远程命令结束后，会通过本地控制 socket 通知 daemon 立即同步一次；daemon 也会低频兜底检查远端变化。

占位文件格式示例：

```text
REMOTE-SANDBOX PLACEHOLDER
reason: large remote file
path: outputs/model.bin
remote: server-a:/data/project/outputs/model.bin
size: 2.1 GB
bytes: 2100000000
mtime: 2026-07-06T14:20:00Z
hash: ...
fetch: rsb fetch -- outputs/model.bin
```

拉取单个占位文件：

```bash
rsb fetch outputs/model.bin
# 或者使用 --，支持以 - 开头的路径
rsb fetch -- outputs/model.bin
```

拉取当前工作区内所有占位文件：

```bash
rsb fetch -a
```

如果只是想让 AI 看大文件的一小段，不要整文件拉回本地，用 `peek`：

```bash
rsb peek outputs/train.log --lines 80
rsb peek outputs/train.log --tail 80
```

`peek` 只把远程文件的前/后 N 行输出到终端，不会替换本地占位文件，也不会更新同步 base。

## 忽略与占位

项目根目录可以放置 `.rsbignore` 来控制路径策略。普通规则默认表示 `ignore`；`[placeholder]` 下面的规则表示只在本地保留占位文件。

```gitignore
# 不同步
.git/
.venv/
__pycache__/
*.pyc

# 指定路径保留占位
[placeholder]
checkpoints/
```

说明：

- 没有匹配规则的文件默认走 `sync`。
- 文件开头的普通规则默认表示不同步。
- 默认大文件阈值由 `rsb set placeholder-limit <size>` 控制，内置默认值是 `10MB`。
- `.rsbignore` 里不写 `placeholder-size`。
