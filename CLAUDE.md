# Shadowtree

Applies Phabricator patch stacks to a clean worktree to verify they merge cleanly together.

## What it does
`shadowtree.py` reads a file of Bugzilla bug IDs, looks up their Phabricator revisions, resolves full revision stacks, and applies them sequentially into a git worktree using `moz-phab patch`. It reports per-bug status (review state + apply success) and diagnoses conflicts between overlapping patches.

## Usage
```
BUGZILLA_API_KEY=... PHABRICATOR_API_TOKEN=... python shadowtree.py lists/sec-high.txt
```

Flags: `--debug`, `--no-cache`, `--cache-ttl`, `--out-dir`

## Key details
- Requires `moz-phab` on PATH
- Caches Bugzilla/Phabricator API responses in `.cache/lookups.db` (SQLite, 1hr TTL)
- Bug lists live in `lists/` (one bug ID per line, `#` comments allowed)
- Bug lists require a `# repo: <url>` directive and support optional `# name:`, `# branch: <name>` (default: main), and `# tag: <name>` directives
- Specifying both `# branch:` and `# tag:` in the same file is an error
- Logs go to `out/logs/`
- `out/<repo-name>/` is the bare clone (derived from repo URL); `out/<name>/` is the throwaway worktree (`--out-dir` configurable)
