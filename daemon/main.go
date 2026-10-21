package main

import (
	"context"
	"crypto/subtle"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sandbox-execution-system/shellctl/handlers"
)

// loggingMiddleware logs incoming HTTP requests with timing
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		if r.URL.Path != "/health" {
			log.Printf("[%s] %s %s (%v)", r.Method, r.URL.Path, r.RemoteAddr, time.Since(start))
		}
	})
}

// authMiddleware enforces constant-time Bearer token verification if SHELLCTL_AUTH_TOKEN is configured
func authMiddleware(next http.Handler) http.Handler {
	expectedToken := os.Getenv("SHELLCTL_AUTH_TOKEN")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Health probe is always unauthenticated
		if r.URL.Path == "/health" {
			next.ServeHTTP(w, r)
			return
		}

		if expectedToken != "" {
			token := ""
			authHeader := r.Header.Get("Authorization")
			if strings.HasPrefix(authHeader, "Bearer ") {
				token = strings.TrimPrefix(authHeader, "Bearer ")
			}

			if token == "" || subtle.ConstantTimeCompare([]byte(token), []byte(expectedToken)) != 1 {
				http.Error(w, "Unauthorized: invalid or missing daemon authentication token", http.StatusUnauthorized)
				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

func main() {
	if os.Getenv("SHELLCTL_AUTH_TOKEN") == "" {
		log.Fatal("SHELLCTL_AUTH_TOKEN must be set; refusing to start without daemon authentication")
	}

	port := os.Getenv("SHELLCTL_PORT")
	if port == "" {
		port = "5004"
	}

	host := os.Getenv("SHELLCTL_HOST")
	if host == "" {
		host = "0.0.0.0"
	}

	workspaceDir := os.Getenv("WORKSPACE_DIR")
	if workspaceDir == "" {
		workspaceDir = "/workspace"
	}

	// Ensure workspace directory exists
	if err := os.MkdirAll(workspaceDir, 0755); err != nil {
		log.Printf("Warning: Could not create workspace directory %s: %v", workspaceDir, err)
	}

	mux := http.NewServeMux()

	// Core Endpoints
	mux.HandleFunc("/health", handlers.HealthHandler)
	mux.HandleFunc("/execute", handlers.ExecuteHandler)
	mux.HandleFunc("/exec", handlers.ExecHandler)
	mux.HandleFunc("/ws/pty", handlers.PTYHandler)

	// Filesystem Endpoints
	mux.HandleFunc("/fs/write", handlers.FSWriteHandler)
	mux.HandleFunc("/fs/read", handlers.FSReadHandler)
	mux.HandleFunc("/fs/list", handlers.FSListHandler)
	mux.HandleFunc("/fs/delete", handlers.FSDeleteHandler)
	mux.HandleFunc("/fs/quota", handlers.FSQuotaHandler)

	addr := fmt.Sprintf("%s:%s", host, port)
	server := &http.Server{
		Addr:         addr,
		Handler:      loggingMiddleware(authMiddleware(mux)),
		ReadTimeout:  120 * time.Second,
		WriteTimeout: 0, // Disabled at server level to permit long-running commands (up to 1800s); deadlines enforced per-request via http.ResponseController
	}

	// Handle graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("🚀 shellctl daemon listening on %s (workspace: %s)", addr, workspaceDir)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("shellctl failed: %v", err)
		}
	}()

	<-stop
	log.Println("Shutting down shellctl daemon gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Forced shutdown error: %v", err)
	}
	log.Println("shellctl exited cleanly.")
}
