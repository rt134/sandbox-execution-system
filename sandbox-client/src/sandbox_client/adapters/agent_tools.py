from __future__ import annotations

from typing import Any, Callable, Dict
from ..session import SandboxSession


def create_agent_tools(session: SandboxSession) -> Dict[str, Callable]:
    """
    Creates ready-to-use tool functions for Autonomous Agent frameworks
    (e.g., Pydantic AI, LangChain, OpenAI function calling).
    """

    async def bash(command: str) -> str:
        """Run a bash command inside the sandbox workspace."""
        res = await session.exec(command)
        out = res.stdout
        if res.stderr:
            out += ("\n" if out else "") + f"[stderr]: {res.stderr}"
        if res.exit_code != 0:
            out += f"\n[exit code: {res.exit_code}]"
        return out or "(no output)"

    async def write_file(path: str, content: str) -> str:
        """Create or overwrite a file in the workspace."""
        await session.fs.write(path, content)
        return f"Successfully wrote {len(content)} bytes to {path}"

    async def read_file(path: str) -> str:
        """Read the contents of a file in the workspace."""
        return await session.fs.read(path)

    async def list_files(path: str = ".") -> str:
        """List files and directories in the workspace."""
        items = await session.fs.list(path)
        if not items:
            return f"Directory '{path}' is empty."
        lines = []
        for item in items:
            kind = "[DIR] " if item.is_dir else "[FILE]"
            lines.append(f"{kind} {item.name} ({item.size} bytes)")
        return "\n".join(lines)

    return {
        "bash": bash,
        "write_file": write_file,
        "read_file": read_file,
        "list_files": list_files,
    }
