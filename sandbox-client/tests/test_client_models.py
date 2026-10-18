import pytest
from sandbox_client.models import CodeResult, ExecResult, ExecChunk, FileInfo, SessionInfo


def test_models_serialization():
    code_res = CodeResult(
        output={"value": 100},
        logs="hello logs",
        exit_code=0,
        duration_ms=12.5,
        sandbox_id="test_sbx",
    )
    assert code_res.output["value"] == 100
    assert code_res.duration_ms == 12.5

    exec_res = ExecResult(stdout="success", exit_code=0, duration_ms=45.0)
    assert exec_res.stdout == "success"

    chunk = ExecChunk(stream="stdout", text="chunk-1\n")
    assert chunk.stream == "stdout"

    finfo = FileInfo(name="app.py", path="app.py", size=1024, is_dir=False)
    assert finfo.size == 1024
    assert not finfo.is_dir
