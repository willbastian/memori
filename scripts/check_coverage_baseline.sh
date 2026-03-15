#!/usr/bin/env bash

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
baseline_file="${MEMORI_COVERAGE_BASELINE_FILE:-${repo_root}/.github/coverage-baseline.txt}"

if [[ ! -f "${baseline_file}" ]]; then
  echo "Coverage baseline file not found: ${baseline_file}" >&2
  exit 1
fi

baseline="$(tr -d '[:space:]' < "${baseline_file}")"
if [[ ! "${baseline}" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
  echo "Coverage baseline must be a numeric percentage, got: ${baseline}" >&2
  exit 1
fi

coverprofile="$(mktemp "${TMPDIR:-/tmp}/memori-cover.XXXXXX.out")"
trap 'rm -f "${coverprofile}"' EXIT

echo "Running full Go test suite with coverage..."
(
  cd "${repo_root}"
  go test ./... -coverprofile="${coverprofile}"
)

current_exact="$(
  awk '
    NR > 1 {
      total += $2
      if ($3 > 0) {
        covered += $2
      }
    }
    END {
      if (total == 0) {
        exit 1
      }
      printf "%.6f", (covered / total) * 100
    }
  ' "${coverprofile}"
)"
current_reported="$(go tool cover -func="${coverprofile}" | tail -n 1 | awk '{print $3}')"

echo "Coverage baseline: ${baseline}% exact"
echo "Current coverage: ${current_exact}% exact (${current_reported} reported by go tool cover)"

if ! awk -v current="${current_exact}" -v baseline="${baseline}" 'BEGIN { exit (current + 1e-9 >= baseline ? 0 : 1) }'; then
  echo "Coverage regression detected: ${current_exact}% is below the required baseline of ${baseline}%." >&2
  exit 1
fi

echo "Coverage baseline satisfied."
