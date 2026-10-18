#!/usr/bin/env python3
"""
Lightweight in-container runner for sub-50ms stateless code execution.
Reads execution spec from stdin, runs user code, and returns JSON output.
"""
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

        local_scope = {"inputs": inputs}
        global_scope = {
            "__builtins__": __builtins__,
            "inputs": inputs,
        }

        exit_code = 0
        output = None
        error_msg = None

        sys.stdout = user_stdout
        sys.stderr = user_stderr

        try:
            # Execute user code definitions
            exec(code, global_scope, local_scope)
            
            # If user defined main(inputs) or main(), invoke it
            if "main" in local_scope and callable(local_scope["main"]):
                func = local_scope["main"]
                try:
                    output = func(inputs)
                except TypeError:
                    output = func()
            elif "result" in local_scope:
                output = local_scope["result"]
            else:
                # Return all JSON-serializable keys created in local scope (excluding system keys)
                output = {
                    k: v for k, v in local_scope.items() 
                    if k not in ("inputs", "__builtins__") and not callable(v) and not str(k).startswith("_")
                }
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
            "exit_code": exit_code
        }
        print(json.dumps(response, default=str))
        sys.exit(exit_code)

    except Exception as e:
        sys.stdout = sys.__stdout__
        print(json.dumps({"error": f"Runner internal error: {str(e)}", "exit_code": 1}))
        sys.exit(1)

if __name__ == "__main__":
    run()
