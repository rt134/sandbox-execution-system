from __future__ import annotations

import base64
from typing import List, Union
import httpx

from .models import DiskQuotaExceededError, FileInfo, SandboxError


class FileSystemClient:
    """Provides file operations inside the sandbox workspace."""

    def __init__(self, base_url: str, session_id: str, http_client: httpx.AsyncClient):
        self.base_url = base_url.rstrip("/")
        self.session_id = session_id
        self._http = http_client

    async def write(self, path: str, content: Union[str, bytes]) -> None:
        """Write text or binary content to a file in /workspace."""
        is_base64 = False
        if isinstance(content, bytes):
            payload_content = base64.b64encode(content).decode("ascii")
            is_base64 = True
        else:
            payload_content = content

        resp = await self._http.post(
            f"{self.base_url}/v1/sessions/{self.session_id}/fs/write",
            json={"path": path, "content": payload_content, "is_base64": is_base64},
        )
        if resp.status_code == 413:
            raise DiskQuotaExceededError(f"Workspace disk quota exceeded: {resp.text}")
        if resp.status_code != 200:
            raise SandboxError(f"Failed to write file '{path}': {resp.text}")

    async def read(self, path: str) -> str:
        """Read text file contents from /workspace."""
        resp = await self._http.get(
            f"{self.base_url}/v1/sessions/{self.session_id}/fs/read",
            params={"path": path},
        )
        if resp.status_code != 200:
            raise SandboxError(f"Failed to read file '{path}': {resp.text}")
        return resp.text

    async def read_bytes(self, path: str) -> bytes:
        """Read raw binary contents from /workspace."""
        resp = await self._http.get(
            f"{self.base_url}/v1/sessions/{self.session_id}/fs/read",
            params={"path": path},
        )
        if resp.status_code != 200:
            raise SandboxError(f"Failed to read file '{path}': {resp.text}")
        return resp.content

    async def list(self, path: str = ".") -> List[FileInfo]:
        """List files and folders in /workspace."""
        resp = await self._http.get(
            f"{self.base_url}/v1/sessions/{self.session_id}/fs/list",
            params={"path": path},
        )
        if resp.status_code != 200:
            raise SandboxError(f"Failed to list directory '{path}': {resp.text}")

        data = resp.json()
        return [
            FileInfo(
                name=item["name"],
                path=item["path"],
                size=item["size"],
                is_dir=item["is_dir"],
                mode=item.get("mode", ""),
                modified_at=str(item.get("modified_at", "")),
            )
            for item in data.get("items", [])
        ]

    async def delete(self, path: str, recursive: bool = False) -> None:
        """Delete a file or directory from /workspace."""
        resp = await self._http.post(
            f"{self.base_url}/v1/sessions/{self.session_id}/fs/delete",
            json={"path": path, "recursive": recursive},
        )
        if resp.status_code != 200:
            raise SandboxError(f"Failed to delete '{path}': {resp.text}")

    async def quota(self) -> dict:
        """Get workspace disk usage and quota details."""
        resp = await self._http.get(
            f"{self.base_url}/v1/sessions/{self.session_id}/fs/quota"
        )
        if resp.status_code != 200:
            raise SandboxError(f"Failed to get workspace quota: {resp.text}")
        return resp.json()

