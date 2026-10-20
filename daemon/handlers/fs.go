package handlers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type FSWriteRequest struct {
	Path     string `json:"path"`
	Content  string `json:"content"`
	IsBase64 bool   `json:"is_base64,omitempty"`
}

type FSDeleteRequest struct {
	Path      string `json:"path"`
	Recursive bool   `json:"recursive,omitempty"`
}

type FileItem struct {
	Name       string    `json:"name"`
	Path       string    `json:"path"`
	Size       int64     `json:"size"`
	IsDir      bool      `json:"is_dir"`
	Mode       string    `json:"mode"`
	ModifiedAt time.Time `json:"modified_at"`
}

type FSListResponse struct {
	Path  string     `json:"path"`
	Items []FileItem `json:"items"`
}

func getWorkspaceRoot() string {
	root := os.Getenv("WORKSPACE_DIR")
	if root == "" {
		root = "/workspace"
	}
	cleanRoot, err := filepath.Abs(root)
	if err != nil {
		return "/workspace"
	}
	return cleanRoot
}

func getWorkspaceDiskQuota() int64 {
	quotaMB := 1024 // Default 1024 MB
	if val := os.Getenv("WORKSPACE_DISK_QUOTA_MB"); val != "" {
		var n int
		if _, err := fmt.Sscanf(val, "%d", &n); err == nil && n > 0 {
			quotaMB = n
		}
	}
	return int64(quotaMB) * 1024 * 1024
}

func getWorkspaceDiskUsage() int64 {
	root := getWorkspaceRoot()
	var totalSize int64
	_ = filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err == nil && info != nil && !info.IsDir() {
			totalSize += info.Size()
		}
		return nil
	})
	return totalSize
}

// sanitizePath ensures path does not escape workspace directory, resolving symlinks
func sanitizePath(userPath string) (string, error) {
	root := getWorkspaceRoot()
	evalRoot, err := filepath.EvalSymlinks(root)
	if err == nil {
		root = evalRoot
	}

	var target string
	if filepath.IsAbs(userPath) {
		target = filepath.Clean(userPath)
	} else {
		target = filepath.Clean(filepath.Join(root, userPath))
	}

	// Lexical check: must be contained within workspace root
	rel, err := filepath.Rel(root, target)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("access denied: path '%s' escapes workspace '%s'", userPath, root)
	}

	// Symlink resolution check
	evalTarget, err := filepath.EvalSymlinks(target)
	if err == nil {
		// Target exists: verify resolved target does not escape resolved workspace root
		relEval, err := filepath.Rel(root, evalTarget)
		if err != nil || relEval == ".." || strings.HasPrefix(relEval, ".."+string(filepath.Separator)) {
			return "", fmt.Errorf("access denied: symlink '%s' resolves outside workspace", userPath)
		}
		return evalTarget, nil
	} else if os.IsNotExist(err) {
		// Target does not exist yet (e.g. creating a new file on write).
		// Check nearest existing ancestor directory to prevent writing through a symlinked directory.
		dir := filepath.Dir(target)
		for dir != "/" && dir != "." {
			evalDir, err := filepath.EvalSymlinks(dir)
			if err == nil {
				relEval, err := filepath.Rel(root, evalDir)
				if err != nil || relEval == ".." || strings.HasPrefix(relEval, ".."+string(filepath.Separator)) {
					return "", fmt.Errorf("access denied: symlink ancestor '%s' resolves outside workspace", dir)
				}
				break
			}
			dir = filepath.Dir(dir)
		}
	}

	return target, nil
}

func FSWriteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FSWriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	targetPath, err := sanitizePath(req.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	var data []byte
	if req.IsBase64 {
		decoded, err := base64.StdEncoding.DecodeString(req.Content)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid base64 content: %v", err), http.StatusBadRequest)
			return
		}
		data = decoded
	} else {
		data = []byte(req.Content)
	}

	// Check disk quota before writing
	quota := getWorkspaceDiskQuota()
	currentUsage := getWorkspaceDiskUsage()

	var existingSize int64
	if fi, err := os.Stat(targetPath); err == nil && !fi.IsDir() {
		existingSize = fi.Size()
	}

	delta := int64(len(data)) - existingSize
	if delta > 0 && (currentUsage+delta) > quota {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"error":           "Workspace disk quota exceeded",
			"used_bytes":      currentUsage,
			"quota_bytes":     quota,
			"requested_bytes": len(data),
		})
		return
	}

	// Create parent directory if missing
	dir := filepath.Dir(targetPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create directories: %v", err), http.StatusInternalServerError)
		return
	}

	if err := os.WriteFile(targetPath, data, 0644); err != nil {
		http.Error(w, fmt.Sprintf("Failed to write file: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":     "ok",
		"path":       targetPath,
		"bytes_writ": len(data),
	})
}

func FSReadHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	reqPath := r.URL.Query().Get("path")
	if reqPath == "" {
		http.Error(w, "Missing 'path' query parameter", http.StatusBadRequest)
		return
	}

	targetPath, err := sanitizePath(reqPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	fileInfo, err := os.Stat(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "File not found", http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	if fileInfo.IsDir() {
		http.Error(w, "Target path is a directory, not a file", http.StatusBadRequest)
		return
	}

	file, err := os.Open(targetPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer file.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", fileInfo.Name()))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, file)
}

func FSListHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	reqPath := r.URL.Query().Get("path")
	if reqPath == "" {
		reqPath = "."
	}

	targetPath, err := sanitizePath(reqPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	entries, err := os.ReadDir(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, "Directory not found", http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	items := make([]FileItem, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		relPath, _ := filepath.Rel(getWorkspaceRoot(), filepath.Join(targetPath, entry.Name()))
		items = append(items, FileItem{
			Name:       entry.Name(),
			Path:       relPath,
			Size:       info.Size(),
			IsDir:      entry.IsDir(),
			Mode:       info.Mode().String(),
			ModifiedAt: info.ModTime().UTC(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(FSListResponse{
		Path:  targetPath,
		Items: items,
	})
}

func FSDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req FSDeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	targetPath, err := sanitizePath(req.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	if targetPath == getWorkspaceRoot() {
		http.Error(w, "Cannot delete root workspace directory", http.StatusForbidden)
		return
	}

	var removeErr error
	if req.Recursive {
		removeErr = os.RemoveAll(targetPath)
	} else {
		removeErr = os.Remove(targetPath)
	}

	if removeErr != nil {
		http.Error(w, fmt.Sprintf("Failed to delete: %v", removeErr), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"status": "deleted",
		"path":   targetPath,
	})
}

type FSQuotaResponse struct {
	UsedBytes  int64   `json:"used_bytes"`
	QuotaBytes int64   `json:"quota_bytes"`
	UsedMB     float64 `json:"used_mb"`
	QuotaMB    float64 `json:"quota_mb"`
	UsagePct   float64 `json:"usage_pct"`
}

func FSQuotaHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	used := getWorkspaceDiskUsage()
	quota := getWorkspaceDiskQuota()

	usedMB := float64(used) / (1024.0 * 1024.0)
	quotaMB := float64(quota) / (1024.0 * 1024.0)
	var usagePct float64
	if quota > 0 {
		usagePct = (float64(used) / float64(quota)) * 100.0
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(FSQuotaResponse{
		UsedBytes:  used,
		QuotaBytes: quota,
		UsedMB:     usedMB,
		QuotaMB:    quotaMB,
		UsagePct:   usagePct,
	})
}

