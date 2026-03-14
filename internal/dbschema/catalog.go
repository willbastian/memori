package dbschema

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"strconv"
	"strings"
)

const migrationsDir = "migrations"

type migrationDefinition struct {
	Version  int
	Name     string
	Checksum string
}

//go:embed migrations/*.sql
var migrationsFS embed.FS

func HeadVersion() (int, error) {
	return headVersion()
}

func headVersion() (int, error) {
	entries, err := fs.ReadDir(migrationsFS, migrationsDir)
	if err != nil {
		return 0, fmt.Errorf("read embedded migrations: %w", err)
	}

	maxVersion := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		prefix, _, hasUnderscore := strings.Cut(name, "_")
		if !hasUnderscore {
			continue
		}
		version, err := strconv.Atoi(prefix)
		if err != nil {
			continue
		}
		if version > maxVersion {
			maxVersion = version
		}
	}
	return maxVersion, nil
}

func readMigrationDefinitions() ([]migrationDefinition, error) {
	entries, err := fs.ReadDir(migrationsFS, migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("read embedded migrations: %w", err)
	}

	definitions := make([]migrationDefinition, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		version, migrationName, ok := parseMigrationFilename(name)
		if !ok {
			continue
		}
		body, err := fs.ReadFile(migrationsFS, migrationsDir+"/"+name)
		if err != nil {
			return nil, fmt.Errorf("read embedded migration %q: %w", name, err)
		}
		sum := sha256.Sum256(body)
		definitions = append(definitions, migrationDefinition{
			Version:  version,
			Name:     migrationName,
			Checksum: hex.EncodeToString(sum[:]),
		})
	}
	return definitions, nil
}

func parseMigrationFilename(name string) (int, string, bool) {
	prefix, rest, hasUnderscore := strings.Cut(name, "_")
	if !hasUnderscore {
		return 0, "", false
	}
	version, err := strconv.Atoi(prefix)
	if err != nil {
		return 0, "", false
	}
	migrationName := strings.TrimSuffix(rest, filepathExt(rest))
	if migrationName == "" {
		migrationName = rest
	}
	return version, migrationName, true
}

func filepathExt(name string) string {
	if dot := strings.LastIndexByte(name, '.'); dot >= 0 {
		return name[dot:]
	}
	return ""
}

func migrationDefinitionMap() (map[int]migrationDefinition, error) {
	definitions, err := readMigrationDefinitions()
	if err != nil {
		return nil, err
	}
	byVersion := make(map[int]migrationDefinition, len(definitions))
	for _, definition := range definitions {
		byVersion[definition.Version] = definition
	}
	return byVersion, nil
}
