# Shadowtree

Checks that private Phabricator patch stacks apply cleanly together against a fresh tree.

## Prerequisites

- [uv](https://docs.astral.sh/uv/)
- `moz-phab` on PATH
- API keys: `BUGZILLA_API_KEY` and `PHABRICATOR_API_TOKEN`

## Usage

```
export BUGZILLA_API_KEY=...
export PHABRICATOR_API_TOKEN=...
./shadowtree.py lists/bug-list.txt
```

## Bug lists

One bug ID per line in `lists/`. Lines starting with `#` are comments, except for directives:

```
# name: my-patches
# repo: https://github.com/example/project.git
# branch: main or # tag: FX_101
1234567
1234568
```

`# repo:` is required. `# name:` sets the worktree directory name (defaults to the filename stem). `# branch:` defaults to main. Use `branch` or `tag`, not both.
