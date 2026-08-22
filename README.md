# Lenso Web

Target-owned Web Interfaces and Modules for Lenso vNext.

This repository owns:

- `lenso.ui.contribution@1`, which declares optional application UI metadata;
- `lenso.web.shell@1`, which assembles routes, navigation, contributions, and
  assets;
- the Web Ingress Interface and linked Rust Module; and
- the browser-to-App projection seam used by target-owned Browser Adapters.

It does not own portable Kernel semantics, authentication policy, business
authorization, a global plugin registry, or a cross-App Console.

## Current packages

- `lenso-capability-ui-contribution`
- `lenso-capability-web-shell`

## Verify

```sh
cargo fmt --all -- --check
cargo check --locked --workspace --all-targets
cargo test --locked --workspace
```

Publication remains disabled until the post-extraction baseline and crates.io
Trusted Publishers are reviewed.
