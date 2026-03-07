-- +goose Up

CREATE TABLE IF NOT EXISTS human_auth_credentials (
    credential_id TEXT PRIMARY KEY CHECK(credential_id = 'default'),
    algorithm TEXT NOT NULL CHECK(algorithm = 'pbkdf2-sha256'),
    iterations INTEGER NOT NULL CHECK(iterations >= 310000),
    salt_hex TEXT NOT NULL CHECK(length(salt_hex) >= 32),
    hash_hex TEXT NOT NULL CHECK(length(hash_hex) = 64),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    rotated_by TEXT NOT NULL
);

-- +goose Down
SELECT 1;
