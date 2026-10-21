package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/exec"
	"sync"

	"github.com/creack/pty"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024 * 32,
	WriteBufferSize: 1024 * 32,
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for sandbox internal network
	},
}

type PTYResizeMessage struct {
	Type string `json:"type"` // "resize"
	Cols uint16 `json:"cols"`
	Rows uint16 `json:"rows"`
}

func PTYHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("PTY: WebSocket upgrade failed: %v", err)
		return
	}
	defer conn.Close()

	cwd := r.URL.Query().Get("cwd")
	if cwd == "" {
		cwd = os.Getenv("WORKSPACE_DIR")
		if cwd == "" {
			cwd = "/workspace"
		}
	}

	shell := os.Getenv("SHELL")
	if shell == "" {
		shell = "/bin/bash"
	}

	cmd := exec.Command(shell)
	cmd.Dir = cwd
	cmd.Env = append(scrubInheritedEnv(), "TERM=xterm-256color")

	// Start PTY with default 80x24 window size
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{
		Rows: 24,
		Cols: 80,
	})
	if err != nil {
		_ = conn.WriteMessage(websocket.TextMessage, []byte("Failed to start PTY: "+err.Error()))
		return
	}
	defer func() {
		_ = ptmx.Close()
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()

	var writeMutex sync.Mutex

	// Goroutine: PTY stdout -> WebSocket client
	done := make(chan struct{})
	go func() {
		buf := make([]byte, 1024*4)
		for {
			n, err := ptmx.Read(buf)
			if err != nil {
				close(done)
				return
			}
			writeMutex.Lock()
			writeErr := conn.WriteMessage(websocket.BinaryMessage, buf[:n])
			writeMutex.Unlock()
			if writeErr != nil {
				close(done)
				return
			}
		}
	}()

	// Main loop: WebSocket client -> PTY stdin
	for {
		messageType, p, err := conn.ReadMessage()
		if err != nil {
			break
		}

		// Check if message is a JSON resize event
		if messageType == websocket.TextMessage && len(p) > 0 && p[0] == '{' {
			var resize PTYResizeMessage
			if err := json.Unmarshal(p, &resize); err == nil && resize.Type == "resize" {
				if resize.Cols > 0 && resize.Rows > 0 {
					_ = pty.Setsize(ptmx, &pty.Winsize{
						Cols: resize.Cols,
						Rows: resize.Rows,
					})
					continue
				}
			}
		}

		// Pass raw input (keystrokes, text) to PTY stdin
		if _, err := ptmx.Write(p); err != nil {
			break
		}
	}

	<-done
}
