# Security policy

## Supported versions
offsetkeeper is pre-release (no tag yet). Only the latest commit on `main` is
supported. After v1.0.0, the latest minor release gets security fixes; older
minors do not.

## Reporting a vulnerability
Do not open a public issue or pull request. Use GitHub private vulnerability
reporting: **Security tab → Report a vulnerability** on
<https://github.com/AynSiddiqui/offsetkeeper/security/advisories/new>.

Include the version or commit, a minimal reproduction, and the impact (for
example: a record committed without being settled, or a DLQ record lost).

You can expect an acknowledgement within 7 days and a fix or a status update
within 30 days. Fixes are released with an entry under *Security* in
`CHANGELOG.md` and a GitHub security advisory crediting the reporter, unless
you ask to stay anonymous.

Delivery-semantics bugs (a record lost or committed early) are in scope.
Vulnerabilities in franz-go or Kafka go to those projects.
