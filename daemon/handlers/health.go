package handlers

import (
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	"time"
)

type HealthResponse struct {
	Status    string    `json:"status"`
	UptimeSec float64   `json:"uptime_sec"`
	GoVersion string    `json:"go_version"`
	Hostname  string    `json:"hostname"`
	Timestamp time.Time `json:"timestamp"`
}

var startTime = time.Now()

func HealthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	hostname, _ := os.Hostname()
	resp := HealthResponse{
		Status:    "ok",
		UptimeSec: time.Since(startTime).Seconds(),
		GoVersion: runtime.Version(),
		Hostname:  hostname,
		Timestamp: time.Now().UTC(),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(resp)
}
