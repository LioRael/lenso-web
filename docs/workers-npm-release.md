# Workers npm releases

Run `release-workers-npm.yml` on `main` with the reviewed package directory,
exact manifest version, and `publish=false` first. The workflow tests and packs
the artifact and reports its SHA-256. Review the source commit and successful
run before dispatching the same source with `publish=true`. A changed main
commit requires another dry run. Publication uses npm OIDC; no token fallback
is configured. Configure each package's Trusted Publisher for this repository
and `release-workers-npm.yml` (no environment).

A first package allocation requires an npm owner bootstrap before Trusted
Publishing is available. Use the reviewed artifact and an owner-authorized
interactive session; never put a long-lived token in this repository. Then
configure Trusted Publishing and verify the exact registry version, integrity,
and provenance. Workflow success alone does not establish consumer adoption.

Versions are explicit in each package manifest. Never overwrite or reuse an
existing version. Publish Runtime before Web's dependent test suite. Consumer
lockfile updates and Workers deployment are separate reviewed changes.
