# JARB project instructions

These instructions are scoped only to the `jarb` project in this repository. Do not reuse them for unrelated projects.

## Canonical GitHub repository

- Repository: `https://github.com/explorelabz/jarb`
- Git remote: `origin`
- All GitHub operations for this project (fetch, pull, push, branches, issues, pull requests, releases, and CI) must target this repository unless the user explicitly overrides it.
- Before any push, release, or deployment, verify that `origin` still resolves to the canonical repository above.

## Default deployment target

- Host: `43.133.204.166`
- SSH account: `ubuntu`
- Treat this server as the default and only deployment target for `jarb` unless the user explicitly names another target.
- Deployment credentials are stored locally in `.deploy.env`, which is intentionally ignored by Git. Never print, commit, copy into logs, or add the password to documentation.
- Before deploying, confirm the intended branch/commit, inspect the current server deployment layout and process manager, and preserve the existing rollback path. Do not guess service names or remote paths.
- After deploying, verify application health and report the deployed commit SHA.

### Established production layout

- Public endpoint: `http://43.133.204.166`
- Versioned releases: `/opt/jarb/releases/<full-commit-sha>`
- Active release symlink: `/opt/jarb/current`
- Persistent runtime data: `/opt/jarb/shared/data`, linked into each release as `data`
- Runtime environment file: `/etc/jarb/jarb.env` (root-owned, mode `0600`)
- Backend service: `jarb.service`, managed by systemd and bound to `127.0.0.1:8787`
- Frontend/proxy: Nginx on port `80`, with `/api/` proxied to the backend
- Nginx site configuration: `/etc/nginx/sites-available/jarb`
- Default deployed mode is `paper`. Never change it to `live` or add exchange credentials without explicit user authorization.
- For subsequent deployments, build and verify a new versioned release first, atomically repoint `current`, restart `jarb.service`, and retain the previous release directory for rollback.

## Scope and safety

- Keep all deployment and GitHub decisions tied to this repository and the JARB system described in `README.md`.
- This is live trading software. A code deployment does not authorize enabling live trading, arming the system, changing exchange credentials, or placing orders.
- Never commit secrets, exchange API keys, server passwords, runtime databases, audit files, or production environment files.
