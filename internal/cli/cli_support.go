package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/willbastian/memori/internal/dbschema"
	"github.com/willbastian/memori/internal/provenance"
	"github.com/willbastian/memori/internal/store"
)

const responseSchemaVersion = 1

var passwordPrompter = readPasswordNoEcho
var authCommandTimeout = 5 * time.Second

type mutationAuthDeps struct {
	resolvePrincipal func() (provenance.Principal, error)
	promptPassword   func(string) (string, error)
}

type mutationIdentity struct {
	Principal provenance.Principal
	Actor     string
	CommandID string
}

func defaultMutationAuthDeps() mutationAuthDeps {
	return mutationAuthDeps{
		resolvePrincipal: provenance.ResolvePrincipal,
		promptPassword:   passwordPrompter,
	}
}

func resolveWriteActor(actorHint string, deps mutationAuthDeps) (provenance.Principal, string, error) {
	principal, err := deps.resolvePrincipal()
	if err != nil {
		return provenance.Principal{}, "", err
	}
	_ = strings.TrimSpace(actorHint)
	return principal, principal.Actor, nil
}

func resolveMutationIdentity(
	ctx context.Context,
	s *store.Store,
	dbPath string,
	operation string,
	actorHint string,
	commandIDHint string,
	deps mutationAuthDeps,
) (mutationIdentity, error) {
	principal, actor, err := resolveWriteActor(actorHint, deps)
	if err != nil {
		return mutationIdentity{}, err
	}
	if principal.Kind == provenance.PrincipalHuman {
		credential, configured, err := s.GetHumanAuthCredential(ctx)
		if err != nil {
			return mutationIdentity{}, err
		}
		if !configured {
			return mutationIdentity{}, fmt.Errorf("human auth is not configured (run: memori auth set-password --db %s)", dbPath)
		}
		password, err := deps.promptPassword("Password: ")
		if err != nil {
			return mutationIdentity{}, err
		}
		ok, err := provenance.VerifyPassword(password, provenance.PasswordCredential{
			Algorithm:  credential.Algorithm,
			Iterations: credential.Iterations,
			SaltHex:    credential.SaltHex,
			HashHex:    credential.HashHex,
		})
		if err != nil {
			return mutationIdentity{}, err
		}
		if !ok {
			return mutationIdentity{}, errors.New("human auth verification failed")
		}
	}

	commandID, err := provenance.ResolveCommandID(operation, commandIDHint)
	if err != nil {
		return mutationIdentity{}, err
	}
	return mutationIdentity{
		Principal: principal,
		Actor:     actor,
		CommandID: commandID,
	}, nil
}

func openInitializedStore(ctx context.Context, dbPath string) (*store.Store, int, error) {
	s, err := store.Open(dbPath)
	if err != nil {
		return nil, 0, err
	}
	v, err := s.SchemaVersion(ctx)
	if err != nil {
		_ = s.Close()
		return nil, 0, err
	}
	if v == 0 {
		_ = s.Close()
		return nil, 0, fmt.Errorf("database is not initialized at %s (run: memori init --db %s)", dbPath, dbPath)
	}
	migrationStatus, err := dbschema.StatusOf(ctx, s.DB())
	if err != nil {
		_ = s.Close()
		return nil, 0, err
	}
	if migrationStatus.PendingMigrations > 0 {
		_ = s.Close()
		return nil, 0, fmt.Errorf(
			"database schema is behind by %d migration(s) (run: memori db migrate --db %s)",
			migrationStatus.PendingMigrations,
			dbPath,
		)
	}
	return s, v, nil
}

func defaultDBPath() string {
	if fromEnv := strings.TrimSpace(os.Getenv("MEMORI_DB_PATH")); fromEnv != "" {
		return fromEnv
	}
	return ".memori/memori.db"
}

func hasFlag(args []string, name string) bool {
	long := "--" + name
	for _, arg := range args {
		if arg == long || strings.HasPrefix(arg, long+"=") {
			return true
		}
	}
	return false
}

type jsonEnvelope struct {
	ResponseSchemaVersion int    `json:"response_schema_version"`
	DBSchemaVersion       int    `json:"db_schema_version"`
	Command               string `json:"command"`
	Data                  any    `json:"data"`
}

type stringSliceFlag []string

func (s *stringSliceFlag) String() string {
	return strings.Join(*s, ",")
}

func (s *stringSliceFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func coalesceIssueKey(key, id string) (string, error) {
	key = strings.TrimSpace(key)
	id = strings.TrimSpace(id)
	if key != "" && id != "" && key != id {
		return "", errors.New("--key and --id were both provided with different values")
	}
	if key != "" {
		return key, nil
	}
	return id, nil
}

func printJSON(out io.Writer, v any) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
