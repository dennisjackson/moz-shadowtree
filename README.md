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

## Output

```
❯ ./shadowtree.py lists/nss-demo.txt
[04/03/26 12:28:49] INFO     📋 8 bug(s) from lists/nss-demo.txt (name: nss-demo)
                    INFO     🔗 Repo: https://github.com/nss-dev/nss.git  Branch: master
[04/03/26 12:28:57] INFO     ✅ Bug 2027388  D290740✅
[04/03/26 12:29:04] INFO     ✅ Bug 2027365  D291630✅ D291631✅
                    INFO     📭 Bug 2027103
[04/03/26 12:29:10] INFO     ✅ Bug 2028001  D291355✅
                    INFO     📭 Bug 1893400
[04/03/26 12:29:20] INFO     ⏳ Bug 2027311  D290741✅ D290743⏳
[04/03/26 12:29:28] INFO     ✅ Bug 2027345  D291082✅ D291083✅
[04/03/26 12:29:36] INFO     ✅ Bug 2027378  D290994✅ D290995✅
                    INFO     🗄️  Cache: 70 hits / 0 misses
                    INFO     ── ✅ 9 applied · ❌ 0 failed · ⏳ 1 unreviewed · 📭 2 no patches ──
                    INFO     📂 Worktree: /home/djackson/Documents/mozilla/shadowtree/out/nss-demo
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
