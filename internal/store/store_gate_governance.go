package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type gateCommandProvenance struct {
	TemplateRef string
	Command     string
	ApprovedBy  string
}

func lookupExecutableGateCommandProvenanceTx(
	ctx context.Context,
	tx *sql.Tx,
	gateSet GateSet,
	gateID string,
) (gateCommandProvenance, error) {
	provenance := gateCommandProvenance{}
	for _, rawRef := range gateSet.TemplateRefs {
		ref, err := parseGateTemplateRef(rawRef)
		if err != nil {
			return gateCommandProvenance{}, fmt.Errorf("validate executable gate governance for %q: %w", gateID, err)
		}

		var (
			definitionJSON string
			approvedBy     string
		)
		err = tx.QueryRowContext(ctx, `
			SELECT t.definition_json, COALESCE(a.approved_by, '')
			FROM gate_templates AS t
			LEFT JOIN gate_template_approvals AS a
				ON a.template_id = t.template_id
				AND a.version = t.version
			WHERE t.template_id = ? AND t.version = ?
		`, ref.TemplateID, ref.Version).Scan(&definitionJSON, &approvedBy)
		if errors.Is(err, sql.ErrNoRows) {
			return gateCommandProvenance{}, fmt.Errorf("gate %q in gate_set %q references missing template %s", gateID, gateSet.GateSetID, ref.Ref)
		}
		if err != nil {
			return gateCommandProvenance{}, fmt.Errorf("lookup template provenance for gate %q in gate_set %q: %w", gateID, gateSet.GateSetID, err)
		}

		defs, err := extractGateDefinitions(definitionJSON)
		if err != nil {
			return gateCommandProvenance{}, fmt.Errorf("decode template provenance for gate %q in gate_set %q: %w", gateID, gateSet.GateSetID, err)
		}
		for _, def := range defs {
			if def.GateID != gateID {
				continue
			}
			if templateCommand := gateCriteriaCommand(def.Criteria); templateCommand != "" {
				provenance.TemplateRef = ref.Ref
				provenance.Command = templateCommand
				provenance.ApprovedBy = approvedBy
			}
		}
	}
	return provenance, nil
}

func executableGateWithoutTemplateProvenanceAllowed(gateSet GateSet) bool {
	return actorIsHumanGoverned(gateSet.CreatedBy)
}

func validateExecutableGateVerificationGovernanceTx(ctx context.Context, tx *sql.Tx, gateSet GateSet, gateID, command string) error {
	command = strings.TrimSpace(command)
	if command == "" {
		return nil
	}

	provenance, err := lookupExecutableGateCommandProvenanceTx(ctx, tx, gateSet, gateID)
	if err != nil {
		return err
	}

	if provenance.Command == command && actorIsHumanGoverned(provenance.ApprovedBy) {
		return nil
	}
	if provenance.Command == "" {
		if executableGateWithoutTemplateProvenanceAllowed(gateSet) {
			return nil
		}
		return fmt.Errorf("gate %q in gate_set %q has executable criteria.command without approved template provenance", gateID, gateSet.GateSetID)
	}
	if provenance.Command != command {
		return fmt.Errorf("gate %q in gate_set %q command does not match approved template provenance", gateID, gateSet.GateSetID)
	}
	return fmt.Errorf("gate %q in gate_set %q uses executable criteria.command from unapproved template %s", gateID, gateSet.GateSetID, provenance.TemplateRef)
}
