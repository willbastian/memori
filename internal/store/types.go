package store

import (
	"database/sql"
	"fmt"
	"strings"
)

type Store struct {
	db *sql.DB
}

type Issue struct {
	ID          string   `json:"id"`
	Type        string   `json:"type"`
	Title       string   `json:"title"`
	ParentID    string   `json:"parent_id,omitempty"`
	Status      string   `json:"status"`
	Priority    string   `json:"priority,omitempty"`
	Labels      []string `json:"labels,omitempty"`
	Description string   `json:"description,omitempty"`
	Acceptance  string   `json:"acceptance_criteria,omitempty"`
	References  []string `json:"references,omitempty"`
	CreatedAt   string   `json:"created_at"`
	UpdatedAt   string   `json:"updated_at"`
	LastEventID string   `json:"last_event_id"`
}

type IssueNextCandidate struct {
	Issue   Issue    `json:"issue"`
	Score   int      `json:"score"`
	Reasons []string `json:"reasons"`
}

type IssueNextResult struct {
	Agent      string               `json:"agent,omitempty"`
	Candidate  IssueNextCandidate   `json:"candidate"`
	Candidates []IssueNextCandidate `json:"candidates"`
	Considered int                  `json:"considered"`
}

type issueNextContinuitySignals struct {
	CurrentCycleNo       int
	OpenLoopCount        int
	FailingRequiredGates int
	BlockedRequiredGates int
	MissingRequiredGates int
	HasFreshPacket       bool
	HasStalePacket       bool
	FocusMatch           bool
	FocusPacketMatch     bool
}

type Event struct {
	EventID             string `json:"event_id"`
	EventOrder          int64  `json:"event_order"`
	EntityType          string `json:"entity_type"`
	EntityID            string `json:"entity_id"`
	EntitySeq           int64  `json:"entity_seq"`
	EventType           string `json:"event_type"`
	PayloadJSON         string `json:"payload_json"`
	Actor               string `json:"actor"`
	CommandID           string `json:"command_id"`
	CausationID         string `json:"causation_id,omitempty"`
	CorrelationID       string `json:"correlation_id,omitempty"`
	CreatedAt           string `json:"created_at"`
	Hash                string `json:"hash"`
	PrevHash            string `json:"prev_hash,omitempty"`
	EventPayloadVersion int    `json:"event_payload_version"`
}

type CreateIssueParams struct {
	IssueID            string
	Type               string
	Title              string
	ParentID           string
	Description        string
	AcceptanceCriteria string
	References         []string
	Actor              string
	CommandID          string
}

type UpdateIssueStatusParams struct {
	IssueID   string
	Status    string
	Actor     string
	CommandID string
}

type UpdateIssueParams struct {
	IssueID            string
	Title              *string
	Status             *string
	Priority           *string
	Labels             *[]string
	Description        *string
	AcceptanceCriteria *string
	References         *[]string
	Actor              string
	CommandID          string
}

type LinkIssueParams struct {
	ChildIssueID  string
	ParentIssueID string
	Actor         string
	CommandID     string
}

type EvaluateGateParams struct {
	IssueID      string
	GateID       string
	Result       string
	EvidenceRefs []string
	Proof        *GateEvaluationProof
	Actor        string
	CommandID    string
}

type GetGateStatusParams struct {
	IssueID string
	CycleNo *int
}

type CheckpointSessionParams struct {
	SessionID string
	IssueID   string
	Trigger   string
	Actor     string
	CommandID string
}

type SummarizeSessionParams struct {
	SessionID string
	Note      string
	Actor     string
	CommandID string
}

type CloseSessionParams struct {
	SessionID string
	Reason    string
	Actor     string
	CommandID string
}

type RehydrateSessionParams struct {
	SessionID string
}

type BuildPacketParams struct {
	Scope     string
	ScopeID   string
	Actor     string
	CommandID string
}

type GetPacketParams struct {
	PacketID string
}

type UsePacketParams struct {
	AgentID   string
	PacketID  string
	Actor     string
	CommandID string
}

type ListOpenLoopsParams struct {
	IssueID string
	CycleNo *int
}

type CreateGateTemplateParams struct {
	TemplateID     string
	Version        int
	AppliesTo      []string
	DefinitionJSON string
	Actor          string
	CommandID      string
}

type ApproveGateTemplateParams struct {
	TemplateID string
	Version    int
	Actor      string
	CommandID  string
}

type ListGateTemplatesParams struct {
	IssueType string
}

type InstantiateGateSetParams struct {
	IssueID      string
	TemplateRefs []string
	Actor        string
	CommandID    string
}

type LockGateSetParams struct {
	IssueID   string
	CycleNo   *int
	Actor     string
	CommandID string
}

type ListIssuesParams struct {
	Type     string
	Status   string
	ParentID string
}

type InitializeParams struct {
	IssueKeyPrefix string
}

type ReplayResult struct {
	EventsApplied int `json:"events_applied"`
}

type appendEventRequest struct {
	EntityType          string
	EntityID            string
	EventType           string
	PayloadJSON         string
	Actor               string
	CommandID           string
	CausationID         string
	CorrelationID       string
	CreatedAt           string
	EventPayloadVersion int
}

type appendEventResult struct {
	Event         Event
	AlreadyExists bool
}

func defaultCorrelationID(entityType, entityID string) string {
	entityType = strings.TrimSpace(entityType)
	entityID = strings.TrimSpace(entityID)
	if entityType == "" || entityID == "" {
		return ""
	}
	return entityType + ":" + entityID
}

func gateTemplateCorrelationID(templateID string, version int) string {
	if strings.TrimSpace(templateID) == "" || version <= 0 {
		return ""
	}
	return fmt.Sprintf("gate-template:%s@%d", templateID, version)
}

func gateCycleCorrelationID(issueID string, cycleNo int) string {
	if strings.TrimSpace(issueID) == "" || cycleNo <= 0 {
		return ""
	}
	return fmt.Sprintf("gate-cycle:%s:%d", issueID, cycleNo)
}

func packetScopeCorrelationID(scope, scopeID string) string {
	scope = strings.TrimSpace(scope)
	scopeID = strings.TrimSpace(scopeID)
	if scope == "" || scopeID == "" {
		return ""
	}
	return fmt.Sprintf("packet-scope:%s:%s", scope, scopeID)
}

type issueCreatedPayload struct {
	IssueID            string   `json:"issue_id"`
	Type               string   `json:"type"`
	Title              string   `json:"title"`
	ParentID           string   `json:"parent_id,omitempty"`
	Status             string   `json:"status"`
	Description        string   `json:"description,omitempty"`
	AcceptanceCriteria string   `json:"acceptance_criteria,omitempty"`
	References         []string `json:"references,omitempty"`
	CreatedAt          string   `json:"created_at"`
}

type issueUpdatedPayload struct {
	IssueID                string                   `json:"issue_id"`
	TitleFrom              *string                  `json:"title_from,omitempty"`
	TitleTo                *string                  `json:"title_to,omitempty"`
	StatusFrom             *string                  `json:"status_from,omitempty"`
	StatusTo               *string                  `json:"status_to,omitempty"`
	PriorityFrom           *string                  `json:"priority_from,omitempty"`
	PriorityTo             *string                  `json:"priority_to,omitempty"`
	LabelsFrom             *[]string                `json:"labels_from,omitempty"`
	LabelsTo               *[]string                `json:"labels_to,omitempty"`
	DescriptionFrom        *string                  `json:"description_from,omitempty"`
	DescriptionTo          *string                  `json:"description_to,omitempty"`
	AcceptanceCriteriaFrom *string                  `json:"acceptance_criteria_from,omitempty"`
	AcceptanceCriteriaTo   *string                  `json:"acceptance_criteria_to,omitempty"`
	ReferencesFrom         *[]string                `json:"references_from,omitempty"`
	ReferencesTo           *[]string                `json:"references_to,omitempty"`
	CloseProof             *IssueCloseAuthorization `json:"close_proof,omitempty"`
	UpdatedAt              string                   `json:"updated_at"`
}

type issueLinkedPayload struct {
	IssueID      string `json:"issue_id"`
	ParentIDFrom string `json:"parent_id_from,omitempty"`
	ParentIDTo   string `json:"parent_id_to"`
	LinkedAt     string `json:"linked_at"`
}

type sessionCheckpointedPayload struct {
	SessionID           string         `json:"session_id"`
	Trigger             string         `json:"trigger"`
	StartedAt           string         `json:"started_at"`
	Checkpoint          map[string]any `json:"checkpoint"`
	CheckpointedAt      string         `json:"checkpointed_at"`
	ContextChunkID      string         `json:"context_chunk_id"`
	ContextChunkKind    string         `json:"context_chunk_kind"`
	ContextChunkContent string         `json:"context_chunk_content"`
	ContextChunkMeta    map[string]any `json:"context_chunk_metadata"`
	CreatedBy           string         `json:"created_by"`
}

type sessionSummarizedPayload struct {
	SessionID           string         `json:"session_id"`
	Summary             map[string]any `json:"summary"`
	SummarizedAt        string         `json:"summarized_at"`
	ContextChunkID      string         `json:"context_chunk_id"`
	ContextChunkKind    string         `json:"context_chunk_kind"`
	ContextChunkContent string         `json:"context_chunk_content"`
	ContextChunkMeta    map[string]any `json:"context_chunk_metadata"`
}

type sessionClosedPayload struct {
	SessionID           string         `json:"session_id"`
	EndedAt             string         `json:"ended_at"`
	SummaryEventID      string         `json:"summary_event_id,omitempty"`
	Reason              string         `json:"reason,omitempty"`
	ClosedAt            string         `json:"closed_at"`
	ContextChunkID      string         `json:"context_chunk_id"`
	ContextChunkKind    string         `json:"context_chunk_kind"`
	ContextChunkContent string         `json:"context_chunk_content"`
	ContextChunkMeta    map[string]any `json:"context_chunk_metadata"`
}

type packetBuiltPayload struct {
	PacketID            string         `json:"packet_id"`
	Scope               string         `json:"scope"`
	Packet              map[string]any `json:"packet"`
	PacketSchemaVersion int            `json:"packet_schema_version"`
	BuiltFromEventID    string         `json:"built_from_event_id,omitempty"`
	CreatedAt           string         `json:"created_at"`
	IssueID             string         `json:"issue_id,omitempty"`
	IssueCycleNo        int            `json:"issue_cycle_no,omitempty"`
}

type focusUsedPayload struct {
	AgentID       string `json:"agent_id"`
	ActiveIssueID string `json:"active_issue_id,omitempty"`
	ActiveCycleNo int    `json:"active_cycle_no,omitempty"`
	LastPacketID  string `json:"last_packet_id"`
	FocusedAt     string `json:"focused_at"`
}

type gateTemplateCreatedPayload struct {
	TemplateID     string   `json:"template_id"`
	Version        int      `json:"version"`
	AppliesTo      []string `json:"applies_to"`
	DefinitionJSON string   `json:"definition_json"`
	DefinitionHash string   `json:"definition_hash"`
	CreatedAt      string   `json:"created_at"`
	CreatedBy      string   `json:"created_by"`
}

type gateTemplateApprovedPayload struct {
	TemplateID     string `json:"template_id"`
	Version        int    `json:"version"`
	DefinitionHash string `json:"definition_hash"`
	ApprovedAt     string `json:"approved_at"`
	ApprovedBy     string `json:"approved_by"`
}

type gateSetInstantiatedPayload struct {
	GateSetID        string              `json:"gate_set_id"`
	IssueID          string              `json:"issue_id"`
	CycleNo          int                 `json:"cycle_no"`
	TemplateRefs     []string            `json:"template_refs"`
	FrozenDefinition map[string]any      `json:"frozen_definition,omitempty"`
	GateSetHash      string              `json:"gate_set_hash"`
	CreatedAt        string              `json:"created_at"`
	CreatedBy        string              `json:"created_by"`
	Items            []GateSetDefinition `json:"items,omitempty"`
}

type gateSetLockedPayload struct {
	GateSetID string `json:"gate_set_id"`
	IssueID   string `json:"issue_id"`
	CycleNo   int    `json:"cycle_no"`
	LockedAt  string `json:"locked_at"`
}

type gateEvaluatedPayload struct {
	IssueID      string               `json:"issue_id"`
	GateSetID    string               `json:"gate_set_id"`
	GateID       string               `json:"gate_id"`
	Result       string               `json:"result"`
	EvidenceRefs []string             `json:"evidence_refs,omitempty"`
	Proof        *GateEvaluationProof `json:"proof,omitempty"`
	EvaluatedAt  string               `json:"evaluated_at"`
}

type GateEvaluationProof struct {
	Verifier      string `json:"verifier"`
	Runner        string `json:"runner"`
	RunnerVersion string `json:"runner_version"`
	ExitCode      int    `json:"exit_code"`
	StartedAt     string `json:"started_at,omitempty"`
	FinishedAt    string `json:"finished_at,omitempty"`
	GateSetHash   string `json:"gate_set_hash,omitempty"`
}

type GateEvaluation struct {
	IssueID      string               `json:"issue_id"`
	GateSetID    string               `json:"gate_set_id"`
	GateID       string               `json:"gate_id"`
	Result       string               `json:"result"`
	EvidenceRefs []string             `json:"evidence_refs,omitempty"`
	Proof        *GateEvaluationProof `json:"proof,omitempty"`
	EvaluatedAt  string               `json:"evaluated_at"`
}

type GateVerificationSpec struct {
	IssueID     string `json:"issue_id"`
	GateSetID   string `json:"gate_set_id"`
	GateSetHash string `json:"gate_set_hash"`
	GateID      string `json:"gate_id"`
	Command     string `json:"command"`
}

type IssueCloseGateProof struct {
	GateID       string               `json:"gate_id"`
	Result       string               `json:"result"`
	EvidenceRefs []string             `json:"evidence_refs,omitempty"`
	Proof        *GateEvaluationProof `json:"proof,omitempty"`
}

type IssueCloseAuthorization struct {
	GateSetID   string                `json:"gate_set_id"`
	GateSetHash string                `json:"gate_set_hash"`
	Gates       []IssueCloseGateProof `json:"gates"`
}

type GateStatus struct {
	IssueID   string           `json:"issue_id"`
	GateSetID string           `json:"gate_set_id"`
	CycleNo   int              `json:"cycle_no"`
	LockedAt  string           `json:"locked_at,omitempty"`
	Gates     []GateStatusItem `json:"gates"`
}

type GateStatusItem struct {
	GateID       string   `json:"gate_id"`
	Kind         string   `json:"kind"`
	Required     bool     `json:"required"`
	Result       string   `json:"result"`
	EvidenceRefs []string `json:"evidence_refs,omitempty"`
	EvaluatedAt  string   `json:"evaluated_at,omitempty"`
	LastEventID  string   `json:"last_event_id,omitempty"`
}

type Session struct {
	SessionID      string         `json:"session_id"`
	Trigger        string         `json:"trigger"`
	StartedAt      string         `json:"started_at"`
	EndedAt        string         `json:"ended_at,omitempty"`
	SummaryEventID string         `json:"summary_event_id,omitempty"`
	Checkpoint     map[string]any `json:"checkpoint,omitempty"`
	CreatedBy      string         `json:"created_by"`
}

type RehydratePacket struct {
	PacketID            string         `json:"packet_id"`
	Scope               string         `json:"scope"`
	Packet              map[string]any `json:"packet"`
	PacketSchemaVersion int            `json:"packet_schema_version"`
	BuiltFromEventID    string         `json:"built_from_event_id,omitempty"`
	CreatedAt           string         `json:"created_at"`
	ScopeID             string         `json:"-"`
	IssueID             string         `json:"-"`
	SessionID           string         `json:"-"`
	IssueCycleNo        int            `json:"-"`
}

type AgentFocus struct {
	AgentID       string `json:"agent_id"`
	ActiveIssueID string `json:"active_issue_id,omitempty"`
	ActiveCycleNo int    `json:"active_cycle_no,omitempty"`
	LastPacketID  string `json:"last_packet_id"`
	UpdatedAt     string `json:"updated_at"`
}

type ContinuitySnapshotParams struct {
	IssueID string
	AgentID string
}

type ContinuitySnapshot struct {
	Issue   IssueContinuitySnapshot   `json:"issue"`
	Agent   AgentContinuitySnapshot   `json:"agent"`
	Session SessionContinuitySnapshot `json:"session"`
}

type IssueContinuitySnapshot struct {
	IssueID        string          `json:"issue_id,omitempty"`
	CurrentCycleNo int             `json:"current_cycle_no,omitempty"`
	LastEventID    string          `json:"last_event_id,omitempty"`
	OpenLoopCount  int             `json:"open_loop_count,omitempty"`
	LatestPacket   RehydratePacket `json:"latest_packet,omitempty"`
	HasPacket      bool            `json:"has_packet"`
	PacketFresh    bool            `json:"packet_fresh"`
	PacketStale    bool            `json:"packet_stale"`
}

type AgentContinuitySnapshot struct {
	AgentID       string          `json:"agent_id,omitempty"`
	Focus         AgentFocus      `json:"focus,omitempty"`
	HasFocus      bool            `json:"has_focus"`
	LastPacket    RehydratePacket `json:"last_packet,omitempty"`
	HasLastPacket bool            `json:"has_last_packet"`
}

type SessionContinuitySnapshot struct {
	Source     string          `json:"source,omitempty"`
	Session    Session         `json:"session,omitempty"`
	HasSession bool            `json:"has_session"`
	Packet     RehydratePacket `json:"packet,omitempty"`
	HasPacket  bool            `json:"has_packet"`
}

type SessionRehydrateResult struct {
	SessionID string          `json:"session_id"`
	Source    string          `json:"source"`
	Packet    RehydratePacket `json:"packet"`
}

type OpenLoop struct {
	LoopID        string `json:"loop_id"`
	IssueID       string `json:"issue_id"`
	CycleNo       int    `json:"cycle_no"`
	LoopType      string `json:"loop_type"`
	Status        string `json:"status"`
	Owner         string `json:"owner,omitempty"`
	Priority      string `json:"priority,omitempty"`
	SourceEventID string `json:"source_event_id,omitempty"`
	UpdatedAt     string `json:"updated_at"`
}

type GateTemplate struct {
	TemplateID     string   `json:"template_id"`
	Version        int      `json:"version"`
	AppliesTo      []string `json:"applies_to"`
	DefinitionJSON string   `json:"definition_json"`
	DefinitionHash string   `json:"definition_hash"`
	Executable     bool     `json:"executable"`
	ApprovedAt     string   `json:"approved_at,omitempty"`
	ApprovedBy     string   `json:"approved_by,omitempty"`
	CreatedAt      string   `json:"created_at"`
	CreatedBy      string   `json:"created_by"`
}

type GateSet struct {
	GateSetID        string              `json:"gate_set_id"`
	IssueID          string              `json:"issue_id"`
	CycleNo          int                 `json:"cycle_no"`
	TemplateRefs     []string            `json:"template_refs"`
	FrozenDefinition map[string]any      `json:"frozen_definition,omitempty"`
	GateSetHash      string              `json:"gate_set_hash"`
	LockedAt         string              `json:"locked_at,omitempty"`
	CreatedAt        string              `json:"created_at"`
	CreatedBy        string              `json:"created_by"`
	Items            []GateSetDefinition `json:"items,omitempty"`
}

type GateSetDefinition struct {
	GateID   string `json:"gate_id"`
	Kind     string `json:"kind"`
	Required bool   `json:"required"`
	Criteria any    `json:"criteria,omitempty"`
}

type HumanAuthCredential struct {
	CredentialID string `json:"credential_id"`
	Algorithm    string `json:"algorithm"`
	Iterations   int    `json:"iterations"`
	SaltHex      string `json:"salt_hex"`
	HashHex      string `json:"hash_hex"`
	CreatedAt    string `json:"created_at"`
	UpdatedAt    string `json:"updated_at"`
	RotatedBy    string `json:"rotated_by"`
}

type UpsertHumanAuthCredentialParams struct {
	Algorithm  string
	Iterations int
	SaltHex    string
	HashHex    string
	Actor      string
}
