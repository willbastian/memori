package dbschema

import (
	"context"
	"database/sql"
	"fmt"
)

func Verify(ctx context.Context, db *sql.DB) (VerifyResult, error) {
	status, err := StatusOf(ctx, db)
	if err != nil {
		return VerifyResult{}, err
	}

	result := VerifyResult{
		OK:             true,
		CurrentVersion: status.CurrentVersion,
		HeadVersion:    status.HeadVersion,
		Checks:         make([]string, 0, 4),
	}

	metaVersion, err := schemaMetaVersion(ctx, db)
	if err != nil {
		result.OK = false
		result.Checks = append(result.Checks, err.Error())
		return result, nil
	}
	result.SchemaMetaVersion = metaVersion

	if status.CurrentVersion == 0 {
		result.OK = false
		result.Checks = append(result.Checks, "database has no applied migrations")
	}
	if metaVersion != status.CurrentVersion {
		result.OK = false
		result.Checks = append(
			result.Checks,
			fmt.Sprintf("schema_meta db_schema_version=%d does not match goose version=%d", metaVersion, status.CurrentVersion),
		)
	}
	if status.CurrentVersion > status.HeadVersion {
		result.OK = false
		result.Checks = append(
			result.Checks,
			fmt.Sprintf("database version %d is ahead of binary head %d", status.CurrentVersion, status.HeadVersion),
		)
	}

	requiredTableFailures, err := verifyRequiredTables(ctx, db, status.CurrentVersion)
	if err != nil {
		result.OK = false
		result.Checks = append(result.Checks, fmt.Sprintf("required table verification failed: %v", err))
		return result, nil
	}
	if len(requiredTableFailures) > 0 {
		result.OK = false
		result.Checks = append(result.Checks, requiredTableFailures...)
	}

	migrationAuditFailures, err := verifyMigrationAudit(ctx, db, status.CurrentVersion)
	if err != nil {
		result.OK = false
		result.Checks = append(result.Checks, fmt.Sprintf("migration audit verification failed: %v", err))
		return result, nil
	}
	if len(migrationAuditFailures) > 0 {
		result.OK = false
		result.Checks = append(result.Checks, migrationAuditFailures...)
	}

	hashChainFailures, err := verifyEventHashChain(ctx, db)
	if err != nil {
		result.OK = false
		result.Checks = append(result.Checks, fmt.Sprintf("event hash-chain verification failed: %v", err))
		return result, nil
	}
	if len(hashChainFailures) > 0 {
		result.OK = false
		result.Checks = append(result.Checks, hashChainFailures...)
	}

	if result.OK {
		result.Checks = append(result.Checks, "schema versions are consistent")
		result.Checks = append(result.Checks, "migration audit matches applied migrations")
		result.Checks = append(result.Checks, "event hash chain is valid")
	}
	return result, nil
}
