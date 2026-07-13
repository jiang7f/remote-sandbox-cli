# remote-sandbox

`remote-sandbox` 将本地目录和 SSH 服务器上的目录绑定为一个双向同步工作区。编辑器和 AI 编码助手只操作本地文件，需要远程环境或算力时通过 `rsb` 执行命令。

正式命令只有 `rsb`。

- 本地和远程同时运行文件 watcher，修改会持续同步。
- 首次同步显示扫描、规划、传输和重放状态。
- 远程 shell 的提示符会动态显示同步状态，例如 `[dev-server:project sync 40%]`。
- 工具元数据保存在工作区之外，不会向项目目录写入 `.remote-sandbox`。
- `.git` 始终留在各自所在的一侧，不参与同步。

## 安装

推荐使用 `uv` 安装独立命令。

```bash
uv tool install remote-sandbox
rsb --help
```

也可以安装到当前 Python 虚拟环境。

```bash
python -m pip install remote-sandbox
rsb --help
```

升级已安装版本。

```bash
uv tool upgrade remote-sandbox
```

## 环境要求

本机需要 Python 3.11、3.12 或 3.13，以及 OpenSSH、`rsync` 和 `tar`。远程服务器需要 Python 3.10 或更高版本，以及 OpenSSH、`bash`、`rsync` 和 `tar`。

先确保普通 SSH 连接可用。建议在 `~/.ssh/config` 中配置主机别名、用户、密钥、端口和跳板机。

```sshconfig
Host dev-server
    HostName server.example.com
    User devuser
    IdentityFile ~/.ssh/id_ed25519
```

`remote-sandbox` 调用系统 OpenSSH，不读取或复制私钥内容。

## 快速开始

### 直接绑定目录

在本地项目目录中执行。

```bash
cd ~/projects/project
rsb connect dev-server \
  --remote ~/work/project \
  --name project
```

确认本地和远程路径后，`rsb` 会立即建立绑定、启动两侧 watcher，并进入远程 shell。首次同步期间，方括号中的状态会原位更新。同步完成后提示符变为 `[dev-server:project]`，shell 不会因为同步结束而自动退出。

如果只想建立绑定而不进入 shell，可以使用 `--no-shell`。

```bash
rsb connect dev-server \
  --remote ~/work/project \
  --local ~/projects/project \
  --name project \
  --no-shell
```

只有本地目录、希望由工具选择安全远程位置时，使用 `--auto-remote`。远程目录会稳定生成在 `~/rsb-workspaces/<项目名>-<短哈希>` 下，并继续遵守首次同步的空目录安全规则。

```bash
rsb connect dev-server \
  --auto-remote \
  --local ~/projects/project \
  --name project \
  --no-shell
```

### 先浏览远程目录

不知道远程目录的准确路径时，可以先进入服务器。

```bash
cd ~/projects/project
rsb enter dev-server
```

在打开的远程 shell 中切换目录，然后发起绑定。

```bash
cd ~/work/project
rsb connect --name project
```

### 首次同步规则

新绑定只接受明确、可判断来源的目录状态。

- 本地非空、远程为空时，本地内容上传到远程。
- 本地为空、远程非空时，远程内容下载到本地。
- 两端都为空时，建立空工作区。
- 两端都非空且没有已验证的历史状态时，拒绝自动合并。

首次传输开始前 watcher 已经启动。扫描期间发生的新修改会进入事件 journal，并在初始快照传输后重放，避免初始化时间较长时漏掉修改。

## 查看状态和路径

```bash
rsb status
rsb status project
rsb status project --paths
rsb status project --watch
```

`--paths` 显示绑定的本地目录和 `<SSH target>:<remote path>`。`--watch` 持续刷新同步阶段、进度、待处理事件、冲突和错误。

常见阶段包括 `initial-syncing`、`syncing`、`ready`、`degraded` 和 `failed`。扫描尚未得到总文件数时显示 `scanning` 或 `collecting`，得到传输计划后才显示百分比。

## 执行远程命令

运行一条远程命令并返回它的退出状态。

```bash
rsb run project -- pytest -q
rsb run project -- python3 train.py
```

打开持续交互的远程 shell。

```bash
rsb shell project
```

如果工作区正在同步，从 `rsb shell` 打开的其他 shell 也会显示同一个动态状态前缀。

## 常用命令

```bash
rsb list
rsb status [name] [--watch] [--paths]
rsb start [name]
rsb stop [name]
rsb shell [name]
rsb run [name] -- <command>
rsb reconnect <name> [--local <path>] [--no-shell]
rsb conflicts [name]
rsb resolve <path> --use-local
rsb resolve <path> --use-remote
rsb fetch <path>
rsb fetch --all
rsb peek <path> --lines 40
rsb peek <path> --tail 40
rsb forget <name>
rsb forget <name> --local-only
```

## 给 AI 编码助手使用

### Codex 推荐方式

安装 `remote-sandbox` 后执行一次。

```bash
rsb skill install
```

该命令把随软件发布的 skill 安装到 `${CODEX_HOME:-~/.codex}/skills/remote-sandbox`。重新打开 Codex 任务后，无需再解释同步方式、本地路径和远程执行的分工。

已知 binding 名称时，直接说任务即可。

```text
用 sfs 修复这个问题，然后跑测试。
```

当 Codex 已经打开 binding 的本地目录时，可以更短。

```text
这个项目在远程跑测试。
```

给出两端目录时，AI 会复用已有的精确 binding，或使用 `--no-shell` 建立新 binding。

```text
把本地 /path/to/project 和 server:~/work/project 绑定，然后继续当前任务。
```

只给本地项目和服务器时，AI 会使用 `--auto-remote` 在 `~/rsb-workspaces` 下选择隔离位置。

skill 默认使用 `rsb run`。它适合普通测试、编译、脚本和长时间训练，能直接得到远程命令的输出和退出码。`rsb shell` 只用于调试器、REPL、`top`、前台服务等真正需要交互或持久状态的工作。AI 不会在每条命令前查询状态，也不会用 `status --watch` 持续监控同步。

完全不提 rsb 时，AI 系统无法仅凭隐藏在用户目录中的 registry 保证触发 skill。最短且稳定的提示是“用 sfs…”或“这个项目远程跑…”。在同一个 Codex 任务里只需说一次。

升级 `remote-sandbox` 后，可以更新已安装的 skill。

```bash
rsb skill install --force
```

不再需要时可以卸载。

```bash
rsb skill uninstall
```

如果 skill 文件被手动修改，卸载会先保护这些修改并提示使用 `--force`。卸载只处理 `rsb` 管理的文件，不会递归删除同一目录中的其他文件。

### 其他 AI 编码助手

不支持 Codex skill 的工具可以读取项目根目录的 `AGENTS.md`。加入以下规则后，不需要在每次任务中重复完整提示。

```markdown
## Remote execution

This project is bound through remote-sandbox as `project`.

- Read and edit only the local working tree.
- Resolve the binding with `rsb status project --paths` once per task.
- Run commands that need the project environment or remote compute with `rsb run project -- <command>`.
- Prefer `rsb run`. Use `rsb shell` only for interactive or stateful programs.
- Do not poll synchronization status unless a command or required generated file is blocked.
- Do not edit the remote workspace directly over SSH.
- Report remote command failures separately from synchronization warnings.
```

AI 编辑代码时仍然直接操作当前本地目录。需要远程依赖、GPU、编译器或测试环境时执行如下命令。

```bash
rsb run project -- pytest -q
```

`rsb run` 会返回远程命令的标准输出、标准错误和退出状态。长时间运行的命令不受内部 SSH 操作超时限制。两端内容重新一致后，陈旧冲突会自动关闭。

## 忽略规则和占位文件

在项目根目录创建默认 `.rsbignore`。

```bash
rsb init
```

普通规则表示忽略。`[placeholder]` 后的规则表示大文件只在本地保留占位信息，需要时再获取。

```gitignore
.venv/
__pycache__/
.pytest_cache/
node_modules/

[placeholder]
checkpoints/
```

设置全局占位阈值。

```bash
rsb set placeholder-limit 100MB
```

`peek` 只读取远程文件的开头或结尾。`fetch` 会拉取并验证占位文件。

## 冲突处理

当本地和远程在同一基线之后修改了同一路径时，`rsb` 保留两侧内容并记录冲突，不会静默覆盖。

```bash
rsb conflicts project
rsb resolve path/to/file --use-local
rsb resolve path/to/file --use-remote
```

## 元数据和清理

项目目录中不会创建 `.remote-sandbox`。本机状态默认保存在以下位置。

```text
~/.remote-sandbox/config.toml
~/.remote-sandbox/connections.toml
~/.remote-sandbox/workspaces/<workspace-id>/workspace.toml
~/.remote-sandbox/workspaces/<workspace-id>/state.sqlite3
~/.remote-sandbox/workspaces/<workspace-id>/daemon.log
```

socket 和 SSH control master 位于 `/tmp/remote-sandbox-<uid>/`。远程 agent 和 workspace 状态位于 `~/.remote-sandbox/`，同样不在远程项目目录内。

测试或隔离环境可以使用 `REMOTE_SANDBOX_HOME`、`REMOTE_SANDBOX_RUNTIME_DIR` 和 `REMOTE_SANDBOX_CONNECTIONS` 覆盖默认位置。

正常删除绑定。

```bash
rsb forget project
```

该命令停止本地 supervisor 和远程 watcher，删除两侧工具元数据及连接记录，但不会删除本地或远程项目文件。

远程服务器不可达时，可以只清理本机记录。

```bash
rsb forget project --local-only
```

命令会报告仍留在远程的工具元数据位置，方便服务器恢复后手动处理。

## 源码开发

```bash
git clone https://github.com/jiang7f/remote-sandbox-cli.git
cd remote-sandbox-cli
uv sync --locked
uv run rsb --help
```

运行代码质量检查和测试。

```bash
uv run ruff check .
uv run mypy src
uv run pytest
```

从当前源码安装可编辑命令。

```bash
uv tool install --editable .
```
