package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"

	"github.com/willbastian/memori/internal/provenance"
	"github.com/willbastian/memori/internal/store"
)

type authStatusData struct {
	Configured bool   `json:"configured"`
	Algorithm  string `json:"algorithm,omitempty"`
	Iterations int    `json:"iterations,omitempty"`
	UpdatedAt  string `json:"updated_at,omitempty"`
	RotatedBy  string `json:"rotated_by,omitempty"`
}

type authSetPasswordData struct {
	Configured bool   `json:"configured"`
	Rotated    bool   `json:"rotated"`
	Algorithm  string `json:"algorithm"`
	Iterations int    `json:"iterations"`
	UpdatedAt  string `json:"updated_at"`
	Actor      string `json:"actor"`
}

func runAuth(args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("auth subcommand required: status|set-password")
	}

	switch args[0] {
	case "status":
		return runAuthStatus(args[1:], out)
	case "set-password":
		return runAuthSetPassword(args[1:], out)
	default:
		return fmt.Errorf("unknown auth subcommand %q", args[0])
	}
}

func runAuthStatus(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("auth status", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), authCommandTimeout)
	defer cancel()

	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	if err != nil {
		return err
	}
	defer s.Close()

	credential, configured, err := s.GetHumanAuthCredential(ctx)
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "auth status",
			Data: authStatusData{
				Configured: configured,
				Algorithm:  credential.Algorithm,
				Iterations: credential.Iterations,
				UpdatedAt:  credential.UpdatedAt,
				RotatedBy:  credential.RotatedBy,
			},
		})
	}

	if !configured {
		_, _ = fmt.Fprintln(out, "Human auth: not configured")
		_, _ = fmt.Fprintf(out, "Run: memori auth set-password --db %s\n", *dbPath)
		return nil
	}

	_, _ = fmt.Fprintln(out, "Human auth: configured")
	_, _ = fmt.Fprintf(out, "Algorithm: %s\n", credential.Algorithm)
	_, _ = fmt.Fprintf(out, "Iterations: %d\n", credential.Iterations)
	_, _ = fmt.Fprintf(out, "Updated: %s\n", credential.UpdatedAt)
	_, _ = fmt.Fprintf(out, "Rotated By: %s\n", credential.RotatedBy)
	return nil
}

func runAuthSetPassword(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("auth set-password", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dbPath := fs.String("db", defaultDBPath(), "sqlite database path")
	jsonOut := fs.Bool("json", false, "machine-readable output")
	if err := fs.Parse(args); err != nil {
		return err
	}

	principal, err := provenance.ResolvePrincipal()
	if err != nil {
		return err
	}
	if principal.Kind != provenance.PrincipalHuman {
		return errors.New("memori auth set-password requires a human principal")
	}

	ctx, cancel := context.WithTimeout(context.Background(), authCommandTimeout)
	s, dbVersion, err := openInitializedStore(ctx, *dbPath)
	cancel()
	if err != nil {
		return err
	}
	defer s.Close()

	ctx, cancel = context.WithTimeout(context.Background(), authCommandTimeout)
	currentCredential, configured, err := s.GetHumanAuthCredential(ctx)
	cancel()
	if err != nil {
		return err
	}
	if configured {
		currentPassword, err := passwordPrompter("Current password: ")
		if err != nil {
			return err
		}
		ok, err := provenance.VerifyPassword(currentPassword, provenance.PasswordCredential{
			Algorithm:  currentCredential.Algorithm,
			Iterations: currentCredential.Iterations,
			SaltHex:    currentCredential.SaltHex,
			HashHex:    currentCredential.HashHex,
		})
		if err != nil {
			return err
		}
		if !ok {
			return errors.New("current password verification failed")
		}
	}

	password, err := passwordPrompter("New password: ")
	if err != nil {
		return err
	}
	confirm, err := passwordPrompter("Confirm password: ")
	if err != nil {
		return err
	}
	if password != confirm {
		return errors.New("password confirmation does not match")
	}

	credential, err := provenance.DerivePasswordCredential(password)
	if err != nil {
		return err
	}
	ctx, cancel = context.WithTimeout(context.Background(), authCommandTimeout)
	storedCredential, rotated, err := s.UpsertHumanAuthCredential(ctx, store.UpsertHumanAuthCredentialParams{
		Algorithm:  credential.Algorithm,
		Iterations: credential.Iterations,
		SaltHex:    credential.SaltHex,
		HashHex:    credential.HashHex,
		Actor:      principal.Actor,
	})
	cancel()
	if err != nil {
		return err
	}

	if *jsonOut {
		return printJSON(out, jsonEnvelope{
			ResponseSchemaVersion: responseSchemaVersion,
			DBSchemaVersion:       dbVersion,
			Command:               "auth set-password",
			Data: authSetPasswordData{
				Configured: true,
				Rotated:    rotated,
				Algorithm:  storedCredential.Algorithm,
				Iterations: storedCredential.Iterations,
				UpdatedAt:  storedCredential.UpdatedAt,
				Actor:      storedCredential.RotatedBy,
			},
		})
	}

	if rotated {
		_, _ = fmt.Fprintln(out, "Rotated human auth password")
	} else {
		_, _ = fmt.Fprintln(out, "Configured human auth password")
	}
	_, _ = fmt.Fprintf(out, "Actor: %s\n", storedCredential.RotatedBy)
	_, _ = fmt.Fprintf(out, "Updated: %s\n", storedCredential.UpdatedAt)
	return nil
}
