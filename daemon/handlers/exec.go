package handlers

import (
	"bufio"
	"bytes"
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

	// Dynamically configure write deadline per request using http.ResponseController
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Now().Add(time.Duration((timeoutSec + 30) * float64(time.Second))))

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

	// Create a new process group for command and all its children
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	// Kill entire process group on timeout immediately
	cmd.Cancel = func() error {
		if cmd.Process != nil && cmd.Process.Pid > 0 {
			return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		return nil
	}

	// Forcibly close I/O pipes after 250ms delay if children hang on stdout/stderr
	cmd.WaitDelay = 250 * time.Millisecond

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

	// Stream output chunk by chunk, handling lines > 1MB without buffer overflow,
	// and draining remaining output to io.Discard once maxOutputBytes is reached.
	streamOutput := func(r io.Reader, streamType string) {
		defer func() {
			_, _ = io.Copy(io.Discard, r)
		}()

		limitedR := io.LimitReader(r, maxOutputBytes)
		reader := bufio.NewReaderSize(limitedR, 64*1024)

		for {
			line, isPrefix, err := reader.ReadLine()
			if len(line) > 0 {
				chunkText := string(line)
				if !isPrefix {
					chunkText += "\n"
				}
				_ = enc.Encode(ExecChunk{
					Stream: streamType,
					Text:   chunkText,
				})
				if canFlush {
					flusher.Flush()
				}
			}
			if err != nil {
				break
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
		if cmd.Process != nil && cmd.Process.Pid > 0 {
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

type cappedBuffer struct {
	limit int
	buf   bytes.Buffer
}

func newCappedBuffer(limit int) *cappedBuffer {
	return &cappedBuffer{limit: limit}
}

func (c *cappedBuffer) Write(p []byte) (n int, err error) {
	if c.buf.Len() < c.limit {
		toWrite := c.limit - c.buf.Len()
		if len(p) < toWrite {
			toWrite = len(p)
		}
		c.buf.Write(p[:toWrite])
	}
	return len(p), nil
}

func (c *cappedBuffer) String() string {
	return c.buf.String()
}

func (c *cappedBuffer) Bytes() []byte {
	return c.buf.Bytes()
}

func (c *cappedBuffer) Len() int {
	return c.buf.Len()
}

func handleBufferedExec(w http.ResponseWriter, cmd *exec.Cmd, ctx context.Context, start time.Time, timeoutSec float64) {
	stdoutBuf := newCappedBuffer(maxOutputBytes)
	stderrBuf := newCappedBuffer(maxOutputBytes)
	cmd.Stdout = stdoutBuf
	cmd.Stderr = stderrBuf

	execErr := cmd.Run()
	durationMs := float64(time.Since(start).Microseconds()) / 1000.0

	exitCode := 0
	var errStr *string

	if ctx.Err() == context.DeadlineExceeded {
		if cmd.Process != nil && cmd.Process.Pid > 0 {
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
		Stdout:     stdoutBuf.String(),
		Stderr:     stderrBuf.String(),
		ExitCode:   exitCode,
		DurationMs: durationMs,
		Error:      errStr,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
