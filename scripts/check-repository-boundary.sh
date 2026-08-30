#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
while IFS=: read -r manifest _ assignment; do
  dependency_path=${assignment#*\"}
  dependency_path=${dependency_path%\"}
  if [[ "$dependency_path" = /* ]]; then
    echo "$manifest: absolute path dependency $dependency_path" >&2
    echo "cross-repository or absolute path dependencies are not allowed" >&2
    exit 1
  fi
  if ! resolved_path=$(cd "$(dirname "$manifest")/$dependency_path" && pwd -P); then
    echo "$manifest: path dependency $dependency_path does not resolve" >&2
    exit 1
  fi
  case "$resolved_path" in
    "$repo_root" | "$repo_root"/*) ;;
    *)
      echo "$manifest: cross-repository path dependency $dependency_path" >&2
      echo "cross-repository or absolute path dependencies are not allowed" >&2
      exit 1
      ;;
  esac
done < <(rg -n --no-heading -o 'path\s*=\s*"[^"]+"' --glob 'Cargo.toml' . || true)

if rg -n 'lenso-(app-plan|kernel|runtime-conformance).*path' --glob 'Cargo.toml' .; then
  echo "portable core must be consumed through released packages" >&2
  exit 1
fi

if rg -n '^(axum|reqwest|tokio)\s*=' crates/lenso-capability-http-*/Cargo.toml; then
  echo "portable HTTP Capability crates must not own native transport dependencies" >&2
  exit 1
fi
