package cli

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/willbastian/memori/internal/dbschema"
)

type versionEnvelope struct {
	ResponseSchemaVersion int    `json:"response_schema_version"`
	DBSchemaVersion       int    `json:"db_schema_version"`
	Command               string `json:"command"`
	Data                  struct {
		Version           string `json:"version"`
		Commit            string `json:"commit"`
		BuildDate         string `json:"build_date"`
		ModulePath        string `json:"module_path"`
		SchemaHeadVersion int    `json:"schema_head_version"`
	} `json:"data"`
}

func TestVersionCommandJSON(t *testing.T) {
	originalVersion := buildVersion
	originalCommit := buildCommit
	originalDate := buildDate
	buildVersion = "v1.2.3"
	buildCommit = "abc1234"
	buildDate = "2026-03-08T18:00:00Z"
	defer func() {
		buildVersion = originalVersion
		buildCommit = originalCommit
		buildDate = originalDate
	}()

	stdout, stderr, err := runMemoriForTest("version", "--json")
	if err != nil {
		t.Fatalf("run version json: %v\nstderr: %s", err, stderr)
	}

	var got versionEnvelope
	if err := json.Unmarshal([]byte(stdout), &got); err != nil {
		t.Fatalf("decode version json: %v\nstdout: %s", err, stdout)
	}
	if got.Command != "version" {
		t.Fatalf("expected version command, got %q", got.Command)
	}
	if got.Data.Version != "v1.2.3" || got.Data.Commit != "abc1234" || got.Data.BuildDate != "2026-03-08T18:00:00Z" {
		t.Fatalf("unexpected version payload: %+v", got.Data)
	}
	if got.Data.ModulePath != buildModulePath {
		t.Fatalf("expected module path %q, got %q", buildModulePath, got.Data.ModulePath)
	}
	headVersion, err := dbschema.HeadVersion()
	if err != nil {
		t.Fatalf("read head version: %v", err)
	}
	if got.Data.SchemaHeadVersion != headVersion || got.DBSchemaVersion != headVersion {
		t.Fatalf("expected schema head version %d, got payload=%d envelope=%d", headVersion, got.Data.SchemaHeadVersion, got.DBSchemaVersion)
	}
}

func TestVersionCommandTextAndHelpAlias(t *testing.T) {
	originalVersion := buildVersion
	buildVersion = "v9.9.9"
	defer func() {
		buildVersion = originalVersion
	}()

	stdout, stderr, err := runMemoriForTest("--version")
	if err != nil {
		t.Fatalf("run --version: %v\nstderr: %s", err, stderr)
	}
	mustContain(t, stdout, "memori v9.9.9")
	mustContain(t, stdout, "module: "+buildModulePath)
	mustContain(t, stdout, "schema_head_version:")

	helpStdout, helpStderr, err := runMemoriForTest("help")
	if err != nil {
		t.Fatalf("run help: %v\nstderr: %s", err, helpStderr)
	}
	if !strings.Contains(helpStdout, "memori version [--json]") {
		t.Fatalf("expected help output to mention version command, got:\n%s", helpStdout)
	}
}
