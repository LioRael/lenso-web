# Agent instructions

This repository owns general-purpose Web backend Interfaces and Modules for
Lenso: inbound HTTP Endpoint/Web Ingress and outbound HTTP Client/Egress.

Keep HTTP transport outside the portable Kernel. Ingress owns listener
lifecycle, protocol parsing, route assembly, transport limits, and network
middleware. Backend Modules provide explicitly bound HTTP Endpoint
Capabilities and remain the final business authority.

Egress owns outbound transport, exact-origin authority, timeouts, redirects,
proxy behavior, retries, and transfer limits. An HTTP Client binding grants
authority only to the configured exact origins. Do not add an allow-all
default, implicit system proxy, automatic redirects, automatic retries, cookie
storage, runtime allowlist mutation, or secret values to immutable Egress
configuration.

Default listeners are loopback-only, but an explicit host configuration may
bind private or public addresses. Do not encode deployment topology as a
framework restriction.

Do not add Console, UI Contribution, Web Shell, Browser Adapter, page, asset,
navigation, or browser plugin ownership here. Those belong to their UI product
owner.

All bindings are immutable before boot. Do not add runtime route registration,
global Capability discovery, fallback providers, or ambient HTTP authority.
Auth owns authentication and target Modules own final authorization; Ingress
only extracts protocol-neutral credential evidence.

The Capability descriptor is authoritative. Regenerate Rust and TypeScript
bindings through `lenso-contract-codegen`; never hand-edit generated files.

Create task worktrees from the latest `origin/main` with
`wt switch --create`. Run Cargo through
`/Users/leosouthey/Projects/framework/.lenso-tools/bin/lenso-cargo` when
available. Use Conventional Commits, stage only requested files, and run the
locked workspace checks plus package dry-runs for changed public crates.
