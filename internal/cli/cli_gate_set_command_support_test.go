package cli

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/willbastian/memori/internal/store"
)

type gateSetCommandFixture struct {
	t      *testing.T
	dbPath string
}

func newGateSetCommandFixture(t *testing.T, dbName string) gateSetCommandFixture {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), dbName)
	if _, stderr, err := runMemoriForTest("init", "--db", dbPath, "--issue-prefix", "mem", "--json"); err != nil {
		t.Fatalf("init db: %v\nstderr: %s", err, stderr)
	}
	return gateSetCommandFixture{t: t, dbPath: dbPath}
}

func (f gateSetCommandFixture) createIssue(issueKey, issueType, title, commandID string) {
	f.t.Helper()

	if _, stderr, err := runMemoriForTest(
		"issue", "create",
		"--db", f.dbPath,
		"--key", issueKey,
		"--type", issueType,
		"--title", title,
		"--command-id", commandID,
		"--json",
	); err != nil {
		f.t.Fatalf("create issue: %v\nstderr: %s", err, stderr)
	}
}

func (f gateSetCommandFixture) writeTemplateDefinition(fileName, definition string) string {
	f.t.Helper()

	defPath := filepath.Join(f.t.TempDir(), fileName)
	if err := os.WriteFile(defPath, []byte(definition), 0o644); err != nil {
		f.t.Fatalf("write template definition file: %v", err)
	}
	return defPath
}

func (f gateSetCommandFixture) createTemplateCLI(templateID string, version int, appliesTo, filePath, commandID string) {
	f.t.Helper()

	if _, stderr, err := runMemoriForTest(
		"gate", "template", "create",
		"--db", f.dbPath,
		"--id", templateID,
		"--version", strconv.Itoa(version),
		"--applies-to", appliesTo,
		"--file", filePath,
		"--command-id", commandID,
		"--json",
	); err != nil {
		f.t.Fatalf("gate template create: %v\nstderr: %s", err, stderr)
	}
}

func (f gateSetCommandFixture) openStore() *store.Store {
	f.t.Helper()

	s, err := store.Open(f.dbPath)
	if err != nil {
		f.t.Fatalf("open store: %v", err)
	}
	return s
}

func (f gateSetCommandFixture) approveTemplateStore(templateID string, version int, commandID string) {
	f.t.Helper()

	s := f.openStore()
	defer s.Close()
	if _, _, err := s.ApproveGateTemplate(context.Background(), store.ApproveGateTemplateParams{
		TemplateID: templateID,
		Version:    version,
		Actor:      "human:will",
		CommandID:  commandID,
	}); err != nil {
		f.t.Fatalf("approve gate template via store: %v", err)
	}
}

func (f gateSetCommandFixture) createTemplateStore(templateID string, version int, appliesTo []string, definitionJSON, commandID string) {
	f.t.Helper()

	s := f.openStore()
	defer s.Close()
	if _, _, err := s.CreateGateTemplate(context.Background(), store.CreateGateTemplateParams{
		TemplateID:     templateID,
		Version:        version,
		AppliesTo:      appliesTo,
		DefinitionJSON: definitionJSON,
		Actor:          "human:will",
		CommandID:      commandID,
	}); err != nil {
		f.t.Fatalf("create gate template via store: %v", err)
	}
}
