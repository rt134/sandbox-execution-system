# Python SDK Reference (`sandbox-client`)

The `sandbox-client` package is a lightweight, asynchronous Python library designed to connect to the `sandbox-engine`.

## Installation

```bash
pip install -e sandbox-client
```

---

## 1. `SandboxClient`

The primary client connection to the central engine.

```python
from sandbox_client import SandboxClient

client = SandboxClient(base_url="http://localhost:8080", timeout=30.0)
```

### Methods

#### `async run_code(language="python", code="", inputs=None, timeout=10.0) -> CodeResult`
Runs stateless code using pre-warmed containers with `< 50ms` latency.
* **Parameters:**
  * `language` *(str)*: Language interpreter (`"python"`, `"node"`, `"bash"`).
  * `code` *(str)*: Source code. If `main(inputs)` is defined, it is automatically invoked.
  * `inputs` *(dict, optional)*: Key-value dictionary passed to `main()`.
  * `timeout` *(float)*: Execution timeout in seconds.
* **Returns:** [`CodeResult`](#coderesult)

#### `acquire_session(workspace_id, session_id=None, ttl_seconds=300) -> SandboxSession`
Async context manager to allocate or resume a stateful agent session. Automatically releases the session on exit.

```python
async with client.acquire_session(workspace_id="my_repo", ttl_seconds=300) as session:
    # All commands run inside this sandbox session
    ...
```

#### `async create_session(workspace_id, session_id=None, ttl_seconds=300) -> SandboxSession`
Creates or attaches to a stateful session without context management. You must call `await session.release()` manually when done.

#### `async aclose()`
Closes the underlying HTTP connection pool.

---

## 2. `SandboxSession`

Represents an active agent environment with a mounted persistent `/workspace`.

### Properties
* `session.id` *(str)*: Unique session ID.
* `session.workspace_id` *(str)*: Workspace identifier.
* `session.fs` *([`FileSystemClient`](#3-filesystemclient-sessionfs))*: Workspace file operations.
* `session.terminal` *([`TerminalClient`](#4-terminalclient-sessionterminal))*: Interactive PTY terminal client.

### Methods

#### `async exec(command, cwd=None, env=None, timeout=300.0) -> ExecResult`
Executes a shell command and buffers complete output.
* **Parameters:**
  * `command` *(str)*: Shell command to execute (e.g., `"pytest -v"`).
  * `cwd` *(str, optional)*: Working directory (defaults to `/workspace`).
  * `env` *(dict, optional)*: Environment variables for this command.
  * `timeout` *(float)*: Command timeout in seconds.
* **Returns:** [`ExecResult`](#execresult)

#### `async exec_stream(command, cwd=None, env=None, timeout=300.0) -> AsyncIterator[ExecChunk]`
Executes a shell command and yields real-time streaming chunks as output is produced.
* **Yields:** [`ExecChunk`](#execchunk)

```python
async for chunk in session.exec_stream("npm install"):
    print(chunk.text, end="")
```

#### `async heartbeat(ttl_seconds=None)`
Extends the session lease by the specified number of seconds.

#### `async release()`
Terminates the sandbox container. Workspace files remain persisted on disk.

---

## 3. `FileSystemClient` (`session.fs`)

Provides secure file operations strictly contained within `/workspace`.

#### `async write(path: str, content: Union[str, bytes]) -> None`
Creates or overwrites a file. Automatically creates any missing parent directories. Accepts text or raw binary bytes.
```python
await session.fs.write("src/utils.py", "def helper(): pass\n")
```

#### `async read(path: str) -> str`
Reads text content of a file.

#### `async read_bytes(path: str) -> bytes`
Reads raw binary bytes of a file.

#### `async list(path: str = ".") -> List[FileInfo]`
Lists directory contents with metadata.
* **Returns:** List of [`FileInfo`](#fileinfo).

#### `async delete(path: str, recursive: bool = False) -> None`
Deletes a file or directory.
```python
await session.fs.delete("temp_folder", recursive=True)
```

---

## 4. `TerminalClient` (`session.terminal`)

Allows opening interactive POSIX PTY sessions.

#### `async open(cols: int = 80, rows: int = 24) -> PTYChannel`
Opens an interactive terminal over WebSocket connected to a live `bash` shell.

### `PTYChannel`
* **`async send(data: Union[str, bytes]) -> None`**: Send keystrokes or text to stdin.
* **`async resize(cols: int, rows: int) -> None`**: Send `SIGWINCH` resize event to adjust window dimensions.
* **`async recv(timeout: Optional[float] = None) -> str`**: Read terminal output chunks (including ANSI escape codes).
* **`async close() -> None`**: Terminate the PTY connection.

```python
pty = await session.terminal.open(cols=100, rows=30)
await pty.send("git log --oneline -n 5\n")
output = await pty.recv(timeout=2.0)
print(output)
await pty.close()
```

---

## 5. Agent Tool Adapters

Pre-built tool definitions ready to pass into AI agent frameworks (e.g. `Pydantic AI`, LangChain, AutoGen):

```python
from sandbox_client import create_agent_tools

tools = create_agent_tools(session)
```

### Generated Tools:
* **`tools["bash"](command: str) -> str`**: Executes a bash command and returns combined stdout/stderr.
* **`tools["write_file"](path: str, content: str) -> str`**: Writes a file to `/workspace`.
* **`tools["read_file"](path: str) -> str`**: Reads a file from `/workspace`.
* **`tools["list_files"](path: str = ".") -> str`**: Returns a formatted list of files and sizes.

---

## 6. Data Models

### `CodeResult`
```python
class CodeResult(BaseModel):
    output: Any = None
    logs: str = ""
    error: Optional[str] = None
    exit_code: int = 0
    duration_ms: float = 0.0
    sandbox_id: Optional[str] = None
```

### `ExecResult`
```python
class ExecResult(BaseModel):
    stdout: str = ""
    stderr: str = ""
    exit_code: int = 0
    duration_ms: float = 0.0
    error: Optional[str] = None
```

### `ExecChunk`
```python
class ExecChunk(BaseModel):
    stream: Optional[str] = None  # "stdout" or "stderr"
    text: str = ""
    exit_code: Optional[int] = None
    error: Optional[str] = None
```

### `FileInfo`
```python
class FileInfo(BaseModel):
    name: str
    path: str
    size: int
    is_dir: bool
    mode: str = ""
    modified_at: str = ""
```
