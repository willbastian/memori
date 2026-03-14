package cli

import "github.com/willbastian/memori/internal/store"

type gateTemplateCreateEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Template   store.GateTemplate `json:"template"`
		Idempotent bool               `json:"idempotent"`
	} `json:"data"`
}

type gateTemplateListEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Count     int                  `json:"count"`
		Templates []store.GateTemplate `json:"templates"`
	} `json:"data"`
}

type gateTemplateApproveEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		Template   store.GateTemplate `json:"template"`
		Idempotent bool               `json:"idempotent"`
	} `json:"data"`
}

type gateSetInstantiateEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		GateSet      store.GateSet `json:"gate_set"`
		Idempotent   bool          `json:"idempotent"`
		AutoSelected bool          `json:"auto_selected"`
	} `json:"data"`
}

type gateSetLockEnvelope struct {
	Command string `json:"command"`
	Data    struct {
		GateSet   store.GateSet `json:"gate_set"`
		LockedNow bool          `json:"locked_now"`
	} `json:"data"`
}
