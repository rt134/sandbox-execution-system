package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"syscall"
	"time"
)

type ExecuteRequest struct {
	Language string                 `json:"language"`
	Code     string                 `json:"code"`
	Inputs   map[string]interface{} `json:"inputs,omitempty"`
	Timeout  float64                `json:"timeout,omitempty"` // seconds
}

type ExecuteResponse struct {
	Output     interface{} `json:"output"`
	Logs       string      `json:"logs"`
	Error      *string     `json:"error,omitempty"`
	ExitCode   int         `json:"exit_code"`
	DurationMs float64     `json:"duration_ms"`
}

func ExecuteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	start := time.Now()

	var req ExecuteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	timeoutSec := req.Timeout
	if timeoutSec <= 0 || timeoutSec > 300 {
		timeoutSec = 10.0 // Default 10s
	}

	// Set dynamic write deadline on ResponseController
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Now().Add(time.Duration((timeoutSec + 15) * float64(time.Second))))

	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(timeoutSec*float64(time.Second)))
	defer cancel()

	runnerPath := os.Getenv("PYTHON_RUNNER_PATH")
	if runnerPath == "" {
		runnerPath = "/opt/runner/python_runner.py"
	}

	var cmd *exec.Cmd
	switch req.Language {
	case "python", "py", "":
		cmd = exec.CommandContext(ctx, "python3", "-u", runnerPath)
	case "node", "javascript", "js":
		// Direct node execution
		cmd = exec.CommandContext(ctx, "node", "-e", req.Code)
	case "bash", "sh":
		cmd = exec.CommandContext(ctx, "bash", "-c", req.Code)
	default:
		errMsg := fmt.Sprintf("Unsupported language: %s", req.Language)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(ExecuteResponse{
			Error:      &errMsg,
			ExitCode:   1,
			DurationMs: float64(time.Since(start).Microseconds()) / 1000.0,
		})
		return
	}

	// Set process group ID and kill entire process group on cancellation
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process != nil && cmd.Process.Pid > 0 {
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}
	cmd.WaitDelay = 250 * time.Millisecond

	// Prepare payload for runner
	payloadBytes, _ := json.Marshal(map[string]interface{}{
		"code":   req.Code,
		"inputs": req.Inputs,
	})

	cmd.Stdin = bytes.NewReader(payloadBytes)
	shared := newSharedOutputCap(maxOutputBytes)
	stdoutBuf := newCappedBuffer(shared)
	stderrBuf := newCappedBuffer(shared)
	cmd.Stdout = stdoutBuf
	cmd.Stderr = stderrBuf

	execErr := cmd.Run()
	durationMs := float64(time.Since(start).Microseconds()) / 1000.0

	// Handle timeout
	if ctx.Err() == context.DeadlineExceeded {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		errMsg := fmt.Sprintf("Execution timed out after %.2f seconds", timeoutSec)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(ExecuteResponse{
			Error:      &errMsg,
			ExitCode:   124,
			DurationMs: durationMs,
			Logs:       stderrBuf.String(),
		})
		return
	}

	exitCode := 0
	if execErr != nil {
		if exitError, ok := execErr.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		} else {
			exitCode = 1
		}
	}

	// If it was python runner, the output is formatted JSON from python_runner.py
	if req.Language == "python" || req.Language == "py" || req.Language == "" {
		var runnerResp struct {
			Output   interface{} `json:"output"`
			Logs     string      `json:"logs"`
			Error    *string     `json:"error"`
			ExitCode int         `json:"exit_code"`
		}

		if err := json.Unmarshal(stdoutBuf.Bytes(), &runnerResp); err == nil {
			resp := ExecuteResponse{
				Output:     runnerResp.Output,
				Logs:       runnerResp.Logs,
				Error:      runnerResp.Error,
				ExitCode:   runnerResp.ExitCode,
				DurationMs: durationMs,
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(resp)
			return
		}
	}

	// Fallback for non-python or unparsed output
	logs := stdoutBuf.String()
	if stderrBuf.Len() > 0 {
		logs += "\n" + stderrBuf.String()
	}

	var errStr *string
	if execErr != nil {
		s := execErr.Error()
		errStr = &s
	}

	resp := ExecuteResponse{
		Output:     stdoutBuf.String(),
		Logs:       logs,
		Error:      errStr,
		ExitCode:   exitCode,
		DurationMs: durationMs,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
