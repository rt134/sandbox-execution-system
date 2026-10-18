package handlers

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

type ExecRequest struct {
	Command string            `json:"command"`
	Cwd     string            `json:"cwd,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	Timeout float64           `json:"timeout,omitempty"` // seconds
	Stream  bool              `json:"stream,omitempty"`
}

type ExecChunk struct {
	Stream   string `json:"stream,omitempty"` // "stdout" or "stderr"
	Text     string `json:"text,omitempty"`
	ExitCode *int   `json:"exit_code,omitempty"`
	Error    string `json:"error,omitempty"`
}

type ExecFullResponse struct {
	Stdout     string  `json:"stdout"`
	Stderr     string  `json:"stderr"`
	ExitCode   int     `json:"exit_code"`
	DurationMs float64 `json:"duration_ms"`
	Error      *string `json:"error,omitempty"`
}

const maxOutputBytes = 10 * 1024 * 1024 // 10 MB limit to prevent daemon memory exhaustion

func ExecHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	start := time.Now()

	var req ExecRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	if req.Command == "" {
		http.Error(w, "Command cannot be empty", http.StatusBadRequest)
		return
	}

	timeoutSec := req.Timeout
	if timeoutSec <= 0 || timeoutSec > 1800 {
		timeoutSec = 300.0 // Default 5 minutes
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(timeoutSec*float64(time.Second)))
	defer cancel()

	cwd := req.Cwd
	if cwd == "" {
		cwd = os.Getenv("WORKSPACE_DIR")
		if cwd == "" {
			cwd = "/workspace"
		}
	}

	cmd := exec.CommandContext(ctx, "bash", "-c", req.Command)
	cmd.Dir = cwd
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// Environment variables
	cmd.Env = os.Environ()
	for k, v := range req.Env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
	}

	if req.Stream {
		handleStreamingExec(w, cmd, ctx, cancel, timeoutSec)
		return
	}

	handleBufferedExec(w, cmd, ctx, start, timeoutSec)
}

func handleStreamingExec(w http.ResponseWriter, cmd *exec.Cmd, ctx context.Context, cancel context.CancelFunc, timeoutSec float64) {
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get stdout pipe: %v", err), http.StatusInternalServerError)
		return
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get stderr pipe: %v", err), http.StatusInternalServerError)
		return
	}

	if err := cmd.Start(); err != nil {
		http.Error(w, fmt.Sprintf("Failed to start command: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	flusher, canFlush := w.(http.Flusher)
	enc := json.NewEncoder(w)

	streamOutput := func(r io.Reader, streamType string) {
		scanner := bufio.NewScanner(io.LimitReader(r, maxOutputBytes))
		buf := make([]byte, 64*1024)
		scanner.Buffer(buf, 1024*1024)

		for scanner.Scan() {
			_ = enc.Encode(ExecChunk{
				Stream: streamType,
				Text:   scanner.Text() + "\n",
			})
			if canFlush {
				flusher.Flush()
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		streamOutput(stdoutPipe, "stdout")
	}()
	go func() {
		defer wg.Done()
		streamOutput(stderrPipe, "stderr")
	}()

	// Wait for pipe readers to complete BEFORE calling cmd.Wait() to prevent races
	wg.Wait()
	execErr := cmd.Wait()

	exitCode := 0
	var errMsg string
	if ctx.Err() == context.DeadlineExceeded {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		exitCode = 124
		errMsg = fmt.Sprintf("Command timed out after %.2f seconds", timeoutSec)
	} else if execErr != nil {
		if exitError, ok := execErr.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		} else {
			exitCode = 1
			errMsg = execErr.Error()
		}
	}

	// Final exit chunk
	_ = enc.Encode(ExecChunk{
		ExitCode: &exitCode,
		Error:    errMsg,
	})
	if canFlush {
		flusher.Flush()
	}
}

func handleBufferedExec(w http.ResponseWriter, cmd *exec.Cmd, ctx context.Context, start time.Time, timeoutSec float64) {
	var stdoutBuf, stderrBuf []byte
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := cmd.Start(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		stdoutBuf, _ = io.ReadAll(io.LimitReader(stdoutPipe, maxOutputBytes))
	}()
	go func() {
		defer wg.Done()
		stderrBuf, _ = io.ReadAll(io.LimitReader(stderrPipe, maxOutputBytes))
	}()

	// Wait for readers to finish reading pipes before cmd.Wait()
	wg.Wait()
	execErr := cmd.Wait()
	durationMs := float64(time.Since(start).Microseconds()) / 1000.0

	exitCode := 0
	var errStr *string

	if ctx.Err() == context.DeadlineExceeded {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		exitCode = 124
		msg := fmt.Sprintf("Timed out after %.2f seconds", timeoutSec)
		errStr = &msg
	} else if execErr != nil {
		if exitError, ok := execErr.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		} else {
			exitCode = 1
		}
		s := execErr.Error()
		errStr = &s
	}

	resp := ExecFullResponse{
		Stdout:     string(stdoutBuf),
		Stderr:     string(stderrBuf),
		ExitCode:   exitCode,
		DurationMs: durationMs,
		Error:      errStr,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
