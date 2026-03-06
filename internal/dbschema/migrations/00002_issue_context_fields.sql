-- +goose Up

ALTER TABLE work_items ADD COLUMN description TEXT NOT NULL DEFAULT '';
ALTER TABLE work_items ADD COLUMN acceptance_criteria TEXT NOT NULL DEFAULT '';
ALTER TABLE work_items ADD COLUMN references_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(references_json));

-- +goose Down
SELECT 1;
