# Shadowtree

See README.md for usage instructions and project overview.

## Development

- Use `uv` to manage dependencies and run the project (e.g. `uv run python shadowtree.py ...`)
- Entry point is `shadowtree.py`; library code lives in `lib/`
  - `lib/api.py` — Phabricator and Bugzilla API clients, HTTP session factory
  - `lib/git.py` — Git operations: clone, worktree, patch application, conflict diagnosis
  - `lib/bugfile.py` — Bug list file parsing
