# Agent instructions

This repository owns the target-owned Web interfaces and Modules for Lenso
vNext: UI Contribution, Web Shell, Web Ingress, and browser-to-App projection.

Keep HTTP transport and browser product concerns outside the portable Kernel.
Ingress owns listener lifecycle, protocol parsing, transport limits, and
network middleware. Browser Adapter owns allowlisted projection from a Web
request to Capabilities resolved by App Composition. Web Shell owns route,
navigation, contribution, and asset assembly.

All bindings are immutable before boot. Do not add runtime route registration,
global Capability discovery, fallback providers, or ambient browser authority.
Auth owns authentication and target Modules own final authorization; Ingress
only extracts protocol-neutral credential evidence.

The Capability descriptor is authoritative. Regenerate Rust and TypeScript
bindings through `lenso-contract-codegen`; never hand-edit generated files.

Create task worktrees from the latest `origin/main` with
`wt switch --create`. Run Cargo through
`/Users/leosouthey/Projects/framework/.lenso-tools/bin/lenso-cargo` when
available. Use Conventional Commits, stage only requested files, and run the
locked workspace checks plus package dry-runs for changed public crates.
