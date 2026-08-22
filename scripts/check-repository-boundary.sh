#!/usr/bin/env bash
set -euo pipefail

if rg -n 'path\s*=\s*"' --glob 'Cargo.toml' .; then
  echo "cross-repository path dependencies are not allowed" >&2
  exit 1
fi

if rg -n 'lenso-(app-plan|kernel|runtime-conformance).*path' --glob 'Cargo.toml' .; then
  echo "portable core must be consumed through released packages" >&2
  exit 1
fi
