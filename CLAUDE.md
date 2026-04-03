# Patch Tree Checker

Applies Phabricator patch stacks to a clean worktree to verify they merge cleanly together.

## What it does
`apply_patches.py` reads a file of Bugzilla bug IDs, looks up their Phabricator revisions, resolves full revision stacks, and applies them sequentially into a git worktree using `moz-phab patch`. It reports per-bug status (review state + apply success) and diagnoses conflicts between overlapping patches.

## Usage
```
BUGZILLA_API_KEY=... PHABRICATOR_API_TOKEN=... python apply_patches.py lists/sec-high.txt
```

Flags: `--debug`, `--no-cache`, `--cache-ttl`, `--repo-dir`, `--worktree-dir`, `--log-dir`

## Key details
- Requires `moz-phab` on PATH
- Caches Bugzilla/Phabricator API responses in `.cache/lookups.db` (SQLite, 1hr TTL)
- Bug lists live in `lists/` (one bug ID per line, `#` comments allowed)
- Bug lists support `# repo: <url>`, `# branch: <name>`, and `# tag: <name>` directives (defaults: nss-dev/nss.git, master)
- Specifying both `# branch:` and `# tag:` in the same file is an error
- Logs go to `logs/`
- `repo/` is the bare clone; `worktree/` is the throwaway worktree (paths configurable via flags)
