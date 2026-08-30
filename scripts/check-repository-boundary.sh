#!/usr/bin/env bash
set -euo pipefail

repo_root=$(git rev-parse --show-toplevel)
suite_auth_root=$(cd "$repo_root/../../lenso-auth-plugin/feat-support-attachment-auth-sdk-alignment" && pwd -P)
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
    "$suite_auth_root/crates/lenso-auth-sdk" | \
    "$suite_auth_root/crates/lenso-capability-auth") ;;
    *)
      echo "$manifest: cross-repository path dependency $dependency_path" >&2
      echo "cross-repository or absolute path dependencies are not allowed" >&2
      exit 1
      ;;
  esac
done < <(rg -n --no-heading -o 'path\s*=\s*"[^"]+"' --glob 'Cargo.toml' . || true)

cargo_bin="${LENSO_CARGO_BIN:-cargo}"
metadata="$($cargo_bin metadata --locked --format-version=1)"
for package in lenso lenso-app-plan lenso-kernel lenso-native-adapter lenso-plugin-authoring lenso-contract-runtime; do
  count="$(jq --arg package "$package" '[.packages[] | select(.name == $package)] | length' <<<"$metadata")"
  if [[ "$count" != "1" ]]; then
    echo "$package resolved $count times; exactly one suite source is required" >&2
    exit 1
  fi
done

if rg -n 'lenso-(app-plan|kernel|runtime-conformance).*path' --glob 'Cargo.toml' .; then
  echo "portable core must be consumed through released packages" >&2
  exit 1
fi

if rg -n '^(axum|reqwest|tokio)\s*=' crates/lenso-capability-http-*/Cargo.toml; then
  echo "portable HTTP Capability crates must not own native transport dependencies" >&2
  exit 1
fi
