# remote-sandbox

`remote-sandbox` 让编辑器只操作本地工作区，同时通过 SSH 在远程工作区执行命令，并由本地 supervisor 维护双向同步。

正式命令只有 `rsb`。工具状态位于 `~/.remote-sandbox`，运行时文件位于 `/tmp/remote-sandbox-<uid>`，项目目录内不会写入控制元数据。

## 安装

使用 `uv` 安装公开发布版。

```bash
uv tool install remote-sandbox
rsb --help
```

## 源码开发

从当前源码安装可编辑版命令。

```bash
uv tool install --editable .
rsb --help
```

在仓库中开发时也可以直接使用 `uv run rsb`。

## 环境要求

本机需要以下工具。

- Python 3.11、3.12 或 3.13
- `uv`
- OpenSSH 客户端
- `rsync`
- `tar`

远程服务器需要以下环境。

- OpenSSH 服务
- Python 3.10 或更高版本
- `bash`
- `rsync`
- `tar`

SSH 主机、端口、用户、密钥和跳板机应配置在 `~/.ssh/config` 中。程序调用系统 OpenSSH，不读取或复制私钥内容。

## 快速开始

```bash
uv sync --locked
uv run rsb --help
uv run rsb list
```

直接绑定已知远程目录。

```bash
uv run rsb connect ZJU_2 \
  --remote ~/work/project \
  --local ./project \
  --name project \
  --no-shell
```

先浏览远程服务器，再从同一个 shell 发起绑定。

```bash
uv run rsb enter ZJU_2
```

进入浏览 shell 后，切换到目标目录并执行终端内提供的 `rsb connect` 请求。

## 常用命令

```bash
uv run rsb list
uv run rsb status [name] [--watch] [--paths]
uv run rsb start [name]
uv run rsb stop [name]
uv run rsb shell [name]
uv run rsb run [name] -- python3 train.py
uv run rsb reconnect <name> [--local <path>] [--no-shell]
uv run rsb conflicts [name]
uv run rsb resolve <path> --use-local
uv run rsb resolve <path> --use-remote
uv run rsb fetch <path>
uv run rsb fetch --all
uv run rsb peek <path> --lines 40
uv run rsb peek <path> --tail 40
uv run rsb forget <name>
uv run rsb forget <name> --local-only
```

查看绑定的本地和远程目录。

```bash
rsb status --paths
rsb status project --paths
```

`REMOTE` 列使用 `<SSH target>:<remote path>` 格式。

`forget <name>` 会停止本地 supervisor，停止远程 watcher，删除远程工具元数据，删除本地工具元数据，并删除连接记录。它不会删除本地或远程项目文件。

`forget <name> --local-only` 用于远程服务器不可达的情况。它只删除本地元数据和连接记录，并明确报告仍然存在的远程工具元数据路径。

## 给 AI 编码助手使用

不需要专门的 skill。AI 只编辑本地工作区，需要运行测试、训练或其他项目命令时，通过 `rsb run` 在远程工作区执行。

临时使用时，可以直接把下面这段发给 AI。

```text
这个项目通过 remote-sandbox 绑定，绑定名是 dq。
只读写当前本地项目目录。
先用 rsb status dq --paths 确认绑定。
运行项目命令时使用 rsb run dq -- <command>。
不要直接通过 SSH 修改远程项目文件。
```

经常使用时，把同样的规则放进项目根目录的 `AGENTS.md`。

```markdown
## Remote execution

This project is bound through remote-sandbox as `dq`.
- Read and edit only the local working tree.
- Check the binding with `rsb status dq --paths`.
- Run project commands with `rsb run dq -- <command>`.
- Do not edit the remote workspace directly over SSH.
```

例如，AI 要在远程运行测试时执行。

```bash
rsb run dq -- pytest -q
```

`rsb shell dq` 适合人工交互操作。对 AI 来说，可返回退出状态的 `rsb run` 更简单也更稳定。

## 元数据隔离

同步项目树中不会写入 `.remote-sandbox` 目录。

本机工具状态默认位于以下位置。

```text
~/.remote-sandbox/config.toml
~/.remote-sandbox/connections.toml
~/.remote-sandbox/workspaces/<workspace-id>/workspace.toml
~/.remote-sandbox/workspaces/<workspace-id>/state.sqlite3
~/.remote-sandbox/workspaces/<workspace-id>/daemon.log
```

本机 socket 和 SSH control master 默认位于以下运行时目录。

```text
/tmp/remote-sandbox-<uid>/
```

远程工具元数据默认位于以下位置。

```text
~/.remote-sandbox/agents/<agent-version>/agent.pyz
~/.remote-sandbox/workspaces/<workspace-id>/
```

测试可以通过 `REMOTE_SANDBOX_HOME`、`REMOTE_SANDBOX_RUNTIME_DIR` 和 `REMOTE_SANDBOX_CONNECTIONS` 指向一次性目录。

## `.git` 规则

`.git` 始终是本地专用内容。

- `.git` 不会上传到远程工作区。
- 远程 `.git` 不会拉回本地工作区。
- `.rsbignore` 不能重新启用 `.git` 同步。
- 工具元数据也不会进入 Git 项目树。

## 首次同步

首次同步只接受以下明确状态。

- 本地非空、远程为空时执行本地到远程同步。
- 本地为空、远程非空时执行远程到本地同步。
- 两端都为空时建立空工作区。
- 两端都非空且没有可恢复的已验证状态时拒绝继续。

首次同步启动 watcher 后再扫描，并在扫描、规划、传输和重放阶段发布状态。传输完成的文件经过源端和目标端验证后才写入 base 和 expected echo 状态。中断后会从已持久化的验证结果继续。

## 增量同步与冲突

本地和远程事件进入 SQLite journal。同步引擎根据 base、local 和 remote 三方状态决定上传、下载、删除、expected echo 或冲突。

查看冲突。

```bash
uv run rsb conflicts project
```

选择保留的一侧。

```bash
uv run rsb resolve path/to/file --use-local
uv run rsb resolve path/to/file --use-remote
```

冲突解决通过 supervisor mutation 队列执行，不会与增量同步引擎并发修改同一路径。

## 忽略和占位文件

在项目根目录运行以下命令可以创建默认 `.rsbignore`。

```bash
uv run rsb init
```

普通规则表示忽略。`[placeholder]` 后的规则表示在本地保留占位文件。

```gitignore
.venv/
__pycache__/
node_modules/

[placeholder]
checkpoints/
```

设置全局占位阈值。

```bash
uv run rsb set placeholder-limit 100MB
```

`peek` 只读取远程文件的前部或尾部，不替换本地占位文件，也不更新同步 base。`fetch` 会通过 supervisor 拉取并验证占位文件。

## 测试

单元和集成测试要求覆盖率不低于 85%。

```bash
uv run pytest tests/unit tests/integration \
  --cov=remote_sandbox \
  --cov-report=term-missing \
  --cov-fail-under=85
```

性能测试使用 5,000 个确定性的 128 字节文件。传输门使用一次预热和五组交替顺序样本，并对中位数应用未放宽的比较公式。

```bash
uv run pytest -q -s tests/performance -m performance
```

Docker SSH E2E 使用一次性 Ubuntu 22.04 容器、随机回环端口、临时 HOME、临时工具状态和临时 Ed25519 密钥。

```bash
RSB_E2E_REQUIRED=1 uv run pytest -q -s tests/e2e -m e2e
```

未设置 `RSB_E2E_REQUIRED=1` 且本机没有 Docker 时，E2E 会明确跳过。设置后缺少 Docker 会直接失败，适合 Linux CI。

## 质量检查

```bash
uv lock --check
uv run ruff check .
uv run mypy src
uv run python -m compileall -q src tests
git diff --check
```

CI 在 macOS 和 Linux 上运行 Python 3.11、3.12 和 3.13 的单元与集成测试。Docker E2E 和性能测试只在 Linux job 中运行。
