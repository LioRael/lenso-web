# Redact rejected Egress origins

Status: IMPLEMENTED AND VALIDATED

Finding: invalid outbound origins are echoed verbatim in configuration errors, including rejected URL user-info that may contain credentials.

Scope:
- preserve exact-origin validation while returning non-secret-bearing errors;
- add a regression proving credentials never appear in diagnostics.

Implementation: rejected Egress origins now use a stable non-echoing diagnostic
while retaining exact parsed-origin validation.

Validation: the credential-redaction regression, all 3 Egress unit tests, both
Egress integration tests, and Web workspace fmt/check/clippy/test passed.
