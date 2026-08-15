#!/usr/bin/env bash
# Enforce the repository's memory-safety policy: any Rust line containing the
# forbidden keyword that is not the mandated `#![forbid(unsafe_code)]`
# attribute fails the check. Scans src/, tests/, and xtask/.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DIRS=("$ROOT/src" "$ROOT/tests" "$ROOT/xtask")

hits=""
for d in "${DIRS[@]}"; do
    if [ -d "$d" ]; then
        hits="$hits$(grep -rn --include='*.rs' 'unsafe' "$d" 2>/dev/null | grep -v 'forbid(unsafe_code)' || true)"
    fi
done

if [ -n "$hits" ]; then
    echo "forbid-unsafe-check: FAILED — policy violations found:" >&2
    printf '%s\n' "$hits" >&2
    exit 1
fi

echo "forbid-unsafe-check: OK — no policy violations in src/, tests/, xtask/"
