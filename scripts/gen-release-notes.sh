#!/usr/bin/env bash
# gen-release-notes.sh — Generate a draft RELEASE_NOTES.md from git log.
#
# Usage:
#   ./scripts/gen-release-notes.sh                    # auto-detect previous tag
#   ./scripts/gen-release-notes.sh v0.1.0             # specify previous tag
#   ./scripts/gen-release-notes.sh v0.1.0 v0.2.0      # specify both tags
#
# Output: writes RELEASE_NOTES.md to the repo root.
#         Existing content is overwritten.
#
# AI Agent workflow:
#   1. Run this script to generate a draft
#   2. Review and edit RELEASE_NOTES.md
#   3. git add RELEASE_NOTES.md && git commit -m "chore: release notes for vX.Y.Z"
#   4. git tag vX.Y.Z && git push origin vX.Y.Z
#      (CI will pick up the tag, run quality checks, build binaries, and create GitHub release)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT="${REPO_ROOT}/RELEASE_NOTES.md"

# ── Resolve tags ──────────────────────────────────────────────────────────────
FROM_TAG="${1:-}"
TO_TAG="${2:-HEAD}"

if [[ -z "$FROM_TAG" ]]; then
  # Auto-detect the most recent semver tag
  FROM_TAG=$(git -C "$REPO_ROOT" describe --tags --abbrev=0 HEAD^ 2>/dev/null || true)
  if [[ -z "$FROM_TAG" ]]; then
    echo "ℹ️  No previous tag found; including all commits." >&2
    FROM_TAG=""
  else
    echo "ℹ️  Auto-detected previous tag: ${FROM_TAG}" >&2
  fi
fi

# ── Collect commits ───────────────────────────────────────────────────────────
if [[ -n "$FROM_TAG" ]]; then
  RANGE="${FROM_TAG}..${TO_TAG}"
else
  RANGE="${TO_TAG}"
fi

echo "ℹ️  Collecting commits: ${RANGE}" >&2

# Get commit list: "<hash> <subject>"
mapfile -t COMMITS < <(git -C "$REPO_ROOT" log --oneline --no-merges "${RANGE}" 2>/dev/null || true)

if [[ ${#COMMITS[@]} -eq 0 ]]; then
  echo "⚠️  No commits found in range ${RANGE}" >&2
fi

# ── Categorize commits ────────────────────────────────────────────────────────
FEATURES=()
FIXES=()
PERF=()
DOCS=()
TESTS=()
REFACTOR=()
CHORE=()
OTHER=()

for commit in "${COMMITS[@]}"; do
  msg="${commit#* }"   # strip hash prefix
  case "$msg" in
    feat:*|feat\(*\):*)    FEATURES+=("- ${msg#*: }") ;;
    fix:*|fix\(*\):*)      FIXES+=("- ${msg#*: }") ;;
    perf:*|perf\(*\):*)    PERF+=("- ${msg#*: }") ;;
    docs:*|docs\(*\):*)    DOCS+=("- ${msg#*: }") ;;
    test:*|test\(*\):*)    TESTS+=("- ${msg#*: }") ;;
    refactor:*|refactor\(*\):*) REFACTOR+=("- ${msg#*: }") ;;
    chore:*|chore\(*\):*)  CHORE+=("- ${msg#*: }") ;;
    *)                     OTHER+=("- ${msg}") ;;
  esac
done

# ── Determine next version ────────────────────────────────────────────────────
if [[ "$TO_TAG" == "HEAD" ]]; then
  NEXT_VERSION="vX.Y.Z  <!-- replace with actual version -->"
else
  NEXT_VERSION="$TO_TAG"
fi

# ── Write RELEASE_NOTES.md ────────────────────────────────────────────────────
{
  echo "# Release ${NEXT_VERSION}"
  echo ""
  echo "> **Draft generated from \`${RANGE}\` on $(date -u +%Y-%m-%d)**"
  echo "> Review, edit, and commit this file before pushing the version tag."
  echo ""

  echo "## What's Changed"
  echo ""

  if [[ ${#FEATURES[@]} -gt 0 ]]; then
    echo "### Features"
    printf '%s\n' "${FEATURES[@]}"
    echo ""
  fi

  if [[ ${#FIXES[@]} -gt 0 ]]; then
    echo "### Bug Fixes"
    printf '%s\n' "${FIXES[@]}"
    echo ""
  fi

  if [[ ${#PERF[@]} -gt 0 ]]; then
    echo "### Performance"
    printf '%s\n' "${PERF[@]}"
    echo ""
  fi

  if [[ ${#REFACTOR[@]} -gt 0 ]]; then
    echo "### Refactoring"
    printf '%s\n' "${REFACTOR[@]}"
    echo ""
  fi

  if [[ ${#DOCS[@]} -gt 0 ]]; then
    echo "### Documentation"
    printf '%s\n' "${DOCS[@]}"
    echo ""
  fi

  if [[ ${#TESTS[@]} -gt 0 ]]; then
    echo "### Tests"
    printf '%s\n' "${TESTS[@]}"
    echo ""
  fi

  if [[ ${#CHORE[@]} -gt 0 ]]; then
    echo "### Chores / Maintenance"
    printf '%s\n' "${CHORE[@]}"
    echo ""
  fi

  if [[ ${#OTHER[@]} -gt 0 ]]; then
    echo "### Other Changes"
    printf '%s\n' "${OTHER[@]}"
    echo ""
  fi

  if [[ ${#COMMITS[@]} -eq 0 ]]; then
    echo "_No commits found in range \`${RANGE}\`. Add change descriptions manually._"
    echo ""
  fi

  echo "## Breaking Changes"
  echo ""
  echo "None"
  echo ""
  echo "## Migration Notes"
  echo ""
  echo "No migration required."
  echo ""
  echo "## Checksums"
  echo ""
  echo "<!-- Populated automatically by CI -->"

} > "$OUTPUT"

echo "✅ Draft written to: ${OUTPUT}" >&2
echo ""
echo "Next steps:" >&2
echo "  1. Review and edit ${OUTPUT}" >&2
echo "  2. git add RELEASE_NOTES.md && git commit -m 'chore: release notes for ${NEXT_VERSION}'" >&2
echo "  3. git tag ${NEXT_VERSION} && git push origin ${NEXT_VERSION}" >&2
