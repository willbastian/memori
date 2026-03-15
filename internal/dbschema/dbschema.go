package dbschema

type Status struct {
	CurrentVersion    int `json:"current_version"`
	HeadVersion       int `json:"head_version"`
	PendingMigrations int `json:"pending_migrations"`
}

type VerifyResult struct {
	OK                bool     `json:"ok"`
	CurrentVersion    int      `json:"current_version"`
	HeadVersion       int      `json:"head_version"`
	SchemaMetaVersion int      `json:"schema_meta_version"`
	Checks            []string `json:"checks"`
}
