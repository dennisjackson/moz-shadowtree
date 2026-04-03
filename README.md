# Shadowtree

Checks that private Phabricator patch stacks apply cleanly together against a fresh tree.

## Prerequisites

- Python 3.10+
- `moz-phab` on PATH
- API keys: `BUGZILLA_API_KEY` and `PHABRICATOR_API_TOKEN`

## Usage

```
export BUGZILLA_API_KEY=...
export PHABRICATOR_API_TOKEN=...
python shadowtree.py lists/sec-high.txt
```

## Bug lists

One bug ID per line in `lists/`. Lines starting with `#` are comments, except for directives:

```
# name: my-patches
# repo: https://github.com/example/project.git
# branch: master
1234567
1234568
```

`# repo:` is required. `# name:` sets the worktree directory name (defaults to the filename stem). `# branch:` defaults to master. Use `branch` or `tag`, not both.

## Options

| Flag | Description |
|---|---|
| `--debug` | Verbose console output |
| `--no-cache` | Skip the API response cache |
| `--cache-ttl N` | Cache lifetime in seconds (default: 3600) |
| `--out-dir DIR` | Output directory for repo and worktrees (default: `./out`) |
| `--log-dir DIR` | Log output directory (default: `./logs`) |
