#!/usr/bin/env python3
"""
Lightweight in-container runner for sub-50ms stateless code execution.
Reads execution spec from stdin, runs user code, and returns JSON output.
"""
import inspect
import io
import json
import sys
import traceback


def run():
    try:
        raw_input = sys.stdin.read()
        if not raw_input:
            print(json.dumps({"error": "Empty input payload", "exit_code": 1}))
            sys.exit(1)

        payload = json.loads(raw_input)
        code = payload.get("code", "")
        inputs = payload.get("inputs", {})

        # Divert stdout and stderr to capture user print logs
        user_stdout = io.StringIO()
        user_stderr = io.StringIO()
        original_stdout = sys.stdout
        original_stderr = sys.stderr

        # Use a single scope dictionary for exec so top-level imports and helper functions
        # are in the exact same namespace as main() (prevents NameError on imports).
        scope = {
            "__builtins__": __builtins__,
            "inputs": inputs,
            "__name__": "__main__",
        }

        exit_code = 0
        output = None
        error_msg = None

        sys.stdout = user_stdout
        sys.stderr = user_stderr

        try:
            # Execute user code definitions in single unified scope
            exec(code, scope)

            # If user defined main(inputs) or main(), inspect signature to invoke properly
            if "main" in scope and callable(scope["main"]):
                func = scope["main"]
                try:
                    sig = inspect.signature(func)
                    params = list(sig.parameters.values())
                    if len(params) == 0:
                        output = func()
                    else:
                        output = func(inputs)
                except ValueError:
                    # In case of built-in functions where signature cannot be inspected
                    try:
                        output = func(inputs)
                    except TypeError:
                        output = func()
            elif "result" in scope:
                output = scope["result"]
            else:
                # Return all JSON-serializable keys created in scope (excluding system keys)
                output = {
                    k: v
                    for k, v in scope.items()
                    if k not in ("inputs", "__builtins__", "__name__", "__doc__", "__package__", "__loader__", "__spec__")
                    and not callable(v)
                    and not str(k).startswith("_")
                }
        except SystemExit as se:
            # Handle user sys.exit() calls cleanly, preserving captured logs and returning valid JSON
            exit_code = se.code if isinstance(se.code, int) else (1 if se.code is not None else 0)
            if se.code is not None and not isinstance(se.code, int):
                error_msg = str(se.code)
        except Exception as e:
            exit_code = 1
            error_msg = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        finally:
            sys.stdout = original_stdout
            sys.stderr = original_stderr

        logs = user_stdout.getvalue()
        if user_stderr.getvalue():
            logs += ("\n" if logs else "") + user_stderr.getvalue()

        response = {
            "output": output,
            "logs": logs,
            "error": error_msg,
            "exit_code": exit_code,
        }
        print(json.dumps(response, default=str))
        sys.exit(exit_code)

    except Exception as e:
        sys.stdout = sys.__stdout__
        print(json.dumps({"error": f"Runner internal error: {str(e)}", "exit_code": 1}))
        sys.exit(1)


if __name__ == "__main__":
    run()
