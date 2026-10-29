package fixtures

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

// Write stores a parser fixture as root/<query>/<caseName>/<file> with a
// meta.json, then scrubs the directory of personal data (keep: ids to
// leave as they are, such as the page id).
func Write(root, query, caseName, file string, body []byte, meta map[string]any, keep []string) (string, error) {
	d := filepath.Join(root, query, caseName)
	if err := os.MkdirAll(d, 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(d, file), body, 0o644); err != nil { //nolint:gosec // fixtures are meant to be committed
		return "", err
	}
	if meta == nil {
		meta = map[string]any{}
	}
	meta["captured_at"] = time.Now().UTC()
	mb, _ := json.MarshalIndent(meta, "", "  ")
	if err := os.WriteFile(filepath.Join(d, "meta.json"), mb, 0o644); err != nil { //nolint:gosec // as above
		return "", err
	}
	return d, ScrubDir(d, keep)
}
