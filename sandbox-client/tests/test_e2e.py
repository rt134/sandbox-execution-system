#!/usr/bin/env python3
"""
End-to-End (E2E) Test Suite for Phase 4:
Validates the sandbox-client SDK against the live sandbox-engine service.
"""

import asyncio
import os
import sys
import httpx
import pytest

from sandbox_engine.main import app
from sandbox_client import SandboxClient, create_agent_tools


@pytest.mark.asyncio
async def test_e2e_full_workflow():
    print("\n" + "=" * 65)
    print("🚀 Running Phase 4: Full End-to-End SDK Integration Tests")
    print("=" * 65)

    # Start sandbox-engine inside lifespan context
    async with app.router.lifespan_context(app):
        # We route client requests through the ASGI transport to test in-memory
        transport = httpx.ASGITransport(app=app)

        async with SandboxClient(base_url="http://test") as client:
            client._http = httpx.AsyncClient(transport=transport, base_url="http://test", timeout=30.0)

            # ==========================================
            # 1. Stateless Code Execution
            # ==========================================
            print("\n1. Testing Stateless Code Execution (run_code)...")
            code_snippet = """
def main(inputs):
    print("Calculating factorials...")
    val = inputs.get("n", 5)
    fact = 1
    for i in range(1, val + 1):
        fact *= i
    return {"number": val, "factorial": fact}
"""
            result = await client.run_code(
                language="python",
                code=code_snippet,
                inputs={"n": 6},
                timeout=5.0,
            )
            print(f"   Stateless result: {result.output}")
            print(f"   Execution duration: {result.duration_ms:.2f}ms (Sandbox: {result.sandbox_id})")
            assert result.output == {"number": 6, "factorial": 720}
            assert "Calculating factorials..." in result.logs
            assert result.exit_code == 0
            print("   ✅ Stateless execution passed!")

            # ==========================================
            # 2. Stateful Agent Session Workflow
            # ==========================================
            print("\n2. Testing Stateful Agent Session (acquire_session)...")
            async with client.acquire_session(workspace_id="agent_e2e_workspace", ttl_seconds=60) as session:
                print(f"   Acquired session: {session.id} (workspace: {session.workspace_id})")

                # A. Write project files into /workspace
                print("   Writing source code and tests via session.fs.write...")
                source_code = "def subtract(a, b):\n    return a - b\n"
                test_code = "from calc import subtract\ndef test_subtract():\n    assert subtract(10, 4) == 6\n"

                await session.fs.write("calc.py", source_code)
                await session.fs.write("test_calc.py", test_code)

                # B. Verify files exist via session.fs.list
                files = await session.fs.list(".")
                file_names = {f.name for f in files}
                assert "calc.py" in file_names
                assert "test_calc.py" in file_names
                print(f"   Files in workspace: {file_names}")

                # C. Stream test execution in real-time
                print("   Running pytest via session.exec_stream...")
                streamed_output = ""
                async for chunk in session.exec_stream("pytest test_calc.py"):
                    streamed_output += chunk.text

                print(f"   Streaming pytest output: {streamed_output.strip()}")
                assert "1 passed" in streamed_output
                print("   ✅ Real-time streaming exec passed!")

                # D. Test Agent Tools Adapter (Pydantic AI style)
                print("   Testing Agent Tools Adapter...")
                tools = create_agent_tools(session)
                bash_tool = tools["bash"]
                write_tool = tools["write_file"]
                read_tool = tools["read_file"]
                list_tool = tools["list_files"]

                write_res = await write_tool("agent_note.txt", "Automated Agent Note")
                assert "Successfully wrote" in write_res

                read_res = await read_tool("agent_note.txt")
                assert read_res == "Automated Agent Note"

                bash_res = await bash_tool("cat agent_note.txt")
                assert "Automated Agent Note" in bash_res

                list_res = await list_tool()
                assert "agent_note.txt" in list_res
                print("   ✅ Agent tools adapter passed!")

            print("   Session released automatically by context manager.")

    print("\n" + "=" * 65)
    print("🎉 ALL PHASE 4 E2E TESTS PASSED SUCCESSFULLY!")
    print("=" * 65)


if __name__ == "__main__":
    asyncio.run(test_e2e_full_workflow())
