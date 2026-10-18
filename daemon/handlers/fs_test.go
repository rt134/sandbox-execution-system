package handlers

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSanitizePath(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "daemon_ws_test")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	os.Setenv("WORKSPACE_DIR", tempDir)
	defer os.Unsetenv("WORKSPACE_DIR")

	root := getWorkspaceRoot()

	tests := []struct {
		name      string
		input     string
		expectErr bool
	}{
		{"valid simple relative", "hello.txt", false},
		{"valid nested relative", "subdir/nested/file.go", false},
		{"valid dot prefix", "./foo/bar.txt", false},
		{"valid absolute within root", filepath.Join(root, "valid.txt"), false},
		{"traversal parent attempt", "../outside.txt", true},
		{"traversal double parent", "../../etc/passwd", true},
		{"traversal embedded", "subdir/../../outside.txt", true},
		{"prefix collision attack", tempDir + "_evil/file.txt", true},
		{"root directly", ".", false},
		{"empty string evaluates to root", "", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cleanPath, err := sanitizePath(tc.input)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("expected error for input '%s', got cleanPath='%s'", tc.input, cleanPath)
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error for input '%s': %v", tc.input, err)
				}
				rel, relErr := filepath.Rel(root, cleanPath)
				if relErr != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
					t.Fatalf("cleanPath '%s' escaped root '%s'", cleanPath, root)
				}
			}
		})
	}
}

func FuzzSanitizePath(f *testing.F) {
	f.Add("normal/file.txt")
	f.Add("../etc/passwd")
	f.Add("....//....//etc/shadow")
	f.Add("/workspace/sub/test.txt")
	f.Add("workspace_evil/payload")
	f.Add("\x00nullbyte")
	f.Add("./../../")

	f.Fuzz(func(t *testing.T, input string) {
		root := getWorkspaceRoot()
		res, err := sanitizePath(input)
		if err == nil {
			rel, relErr := filepath.Rel(root, res)
			if relErr != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				t.Errorf("fuzz input %q escaped root: got %q", input, res)
			}
		}
	})
}
