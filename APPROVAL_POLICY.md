# Approval policy

Used by Cursor [PR Routing & Approval](https://cursor.com/docs/approval-agents).

## CLI sync PRs (auto-approve)

PRs that mirror the API client from `planetscale/cli` into this repo are
mechanical syncs. The client in `planetscale/cli` (`internal/planetscale`) is
the source of truth; this repository is a read-only mirror for external module
users.

Treat a PR as a CLI sync when any of the following match:

- Author is `planetscale-cli[bot]` / `app/planetscale-cli`
- Head branch matches `sync-cli-*`
- Title matches `Sync API client from cli@*`

For these PRs:

- **Approve** after Bugbot has finished reviewing, once required CI is green
  or in progress.
- **Low-severity Bugbot findings are not a hard stop.** Style, consistency,
  and typing nits (for example timestamp typed as `string` vs `time.Time`)
  must not block approval.
- **Do not approve** if Bugbot reports Medium or High severity findings, or
  findings that look like real correctness or security bugs. In that case,
  request review from `@planetscale/surfaces` and leave a short comment.
- Prefer approving over requesting Surfaces for clean or Low-only sync PRs.
- Incompatible `gorelease` entries (added methods/types, signature changes)
  are expected on syncs and do not by themselves require human approval.
  Version tagging after merge remains a separate step.

## All other PRs

Do not auto-approve. Route to `@planetscale/surfaces` (see `CODEOWNERS`).
