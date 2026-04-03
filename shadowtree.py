#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "requests>=2.33.1",
#     "requests-cache>=1.3.1",
#     "rich>=14.3.3",
#     "unidiff>=0.7.5",
# ]
# ///
"""
Clones a repo, looks up Phabricator revisions for each bug in Bugzilla,
checks revision status via Phabricator Conduit, finds the stack tip,
creates a single worktree, and applies each stack via moz-phab patch.
"""

import argparse
import dataclasses
import logging
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path

from rich.logging import RichHandler

from lib.api import (
    DEFAULT_CACHE_DB,
    DEFAULT_CACHE_TTL,
    SKIP_STATUSES,
    PhabClient,
    create_session,
    get_revisions_for_bug,
)
from lib.bugfile import parse_bug_file
from lib.git import apply_stack, clone_repo, create_worktree


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class RunResult:
    succeeded: list[tuple[str, int]] = dataclasses.field(default_factory=list)
    failed: list[tuple[str, int, str]] = dataclasses.field(default_factory=list)
    skipped_bugs: list[str] = dataclasses.field(default_factory=list)
    applied_files: dict[str, list[tuple[str, int]]] = dataclasses.field(default_factory=dict)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Apply Phabricator patches by bug number.",
    )
    parser.add_argument(
        "bug_file", type=Path, help="File with one bug number per line"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("out"),
        help="Output directory for repo and worktrees (default: ./out)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show debug output on the console (always written to log file)",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Disable the persistent lookup cache",
    )
    parser.add_argument(
        "--cache-ttl",
        type=int,
        default=DEFAULT_CACHE_TTL,
        help=f"Cache TTL in seconds (default: {DEFAULT_CACHE_TTL})",
    )
    args = parser.parse_args()

    log_dir = args.out_dir / "logs"
    logger = setup_logging(log_dir, debug=args.debug)

    # Preflight checks
    bz_api_key = os.environ.get("BUGZILLA_API_KEY")
    phab_api_token = os.environ.get("PHABRICATOR_API_TOKEN")
    bug_file: Path = args.bug_file

    missing = []
    if not shutil.which("moz-phab"): missing.append("moz-phab not found on PATH")
    if not shutil.which("git"): missing.append("git not found on PATH")
    if not bz_api_key: missing.append("BUGZILLA_API_KEY not set")
    if not phab_api_token: missing.append("PHABRICATOR_API_TOKEN not set")
    if not bug_file.exists(): missing.append(f"Bug file not found: {bug_file}")
    for msg in missing:
        logger.error("\u274c %s", msg)
    if missing:
        sys.exit(1)

    cfg = parse_bug_file(bug_file)
    logger.info("\U0001f4cb %d bug(s) from %s (name: %s)", len(cfg.bug_ids), bug_file, cfg.name)
    if cfg.tag:
        logger.info("\U0001f517 Repo: %s  Tag: %s", cfg.repo_url, cfg.tag)
    else:
        logger.info("\U0001f517 Repo: %s  Branch: %s", cfg.repo_url, cfg.branch)

    # Clone / update repo
    out_dir = args.out_dir.resolve()
    repo_name = cfg.repo_url.rstrip("/").rsplit("/", 1)[-1].removesuffix(".git")
    repo_dir = out_dir / repo_name
    clone_repo(repo_dir, cfg.repo_url, cfg.branch, logger, tag=cfg.tag)

    # Create a single worktree
    worktree_dir = out_dir / cfg.name
    create_worktree(repo_dir, worktree_dir, cfg.branch, logger, tag=cfg.tag)

    # For each bug: find revisions, resolve stacks, check statuses, apply
    session = create_session(
        cache_db=None if args.no_cache else DEFAULT_CACHE_DB,
        cache_ttl=args.cache_ttl,
    )
    try:
        phab = PhabClient(phab_api_token, session, logger)
        run = _main_loop(cfg.bug_ids, bz_api_key, phab, session, worktree_dir, logger)
    finally:
        session.close()

    logger.info(
        "\U0001f5c4\ufe0f  Cache: %d hits / %d misses",
        session.cache_hits, session.cache_misses,
    )

    logger.info(
        "\u2500\u2500 %d applied \u00b7 %d failed \u00b7 %d no patches \u2500\u2500",
        len(run.succeeded), len(run.failed), len(run.skipped_bugs),
    )
    logger.info("\U0001f4c2 Worktree: %s", worktree_dir)


def _main_loop(
    bug_ids: list[str],
    bz_api_key: str,
    phab: PhabClient,
    session,
    worktree_dir: Path,
    logger: logging.Logger,
) -> RunResult:
    run = RunResult()

    for bug_id in bug_ids:
        logger.debug("\u2500\u2500 Bug %s \u2500\u2500", bug_id)

        rev_ids = get_revisions_for_bug(bug_id, bz_api_key, session, logger)
        if not rev_ids:
            run.skipped_bugs.append(bug_id)
            logger.info("\U0001f4ed Bug %s", bug_id)
            continue

        tips_seen: set[int] = set()
        rev_emojis: dict[int, str] = {}
        tip_applied: dict[int, bool] = {}
        bug_warnings: list[str] = []

        for rev_id in rev_ids:
            try:
                tip_id, stack_revs = phab.find_stack_tip(rev_id)
            except RuntimeError as e:
                run.failed.append((bug_id, rev_id, str(e)))
                rev_emojis[rev_id] = "\u274c"
                tip_applied[rev_id] = False
                bug_warnings.append(f"  D{rev_id}: {e}")
                continue

            entry_rev = next(
                (r for r in stack_revs if r["id"] == rev_id), None
            )
            if entry_rev and entry_rev["fields"]["status"]["value"] in SKIP_STATUSES:
                logger.debug("\U0001f5d1\ufe0f  D%d abandoned, skipping", rev_id)
                continue

            rev_emojis.update(phab.get_revision_status_emojis(stack_revs))

            if tip_id in tips_seen:
                logger.debug("\u23ed\ufe0f  D%d (tip D%d already queued)", rev_id, tip_id)
                continue
            tips_seen.add(tip_id)

            stack_file_map = phab.get_revision_paths(stack_revs)

            ok, modified_files = apply_stack(
                tip_id, worktree_dir, logger,
                stack_file_map=stack_file_map,
                applied_files=run.applied_files,
            )
            tip_applied[tip_id] = ok
            if ok:
                run.succeeded.append((bug_id, tip_id))
                for f in modified_files:
                    run.applied_files.setdefault(f, []).append((bug_id, tip_id))
            else:
                run.failed.append((bug_id, tip_id, "patch failed"))
                bug_warnings.append(f"  D{tip_id}: patch failed to apply")

        parts: list[str] = []
        all_good = True
        for rev_id, emoji in rev_emojis.items():
            if rev_id in tip_applied and not tip_applied[rev_id]:
                emoji = "\u274c"
            if emoji != "\u2705":
                all_good = False
            parts.append(f"D{rev_id}{emoji}")
        bug_emoji = "\u2705" if all_good else "\u26a0\ufe0f "
        logger.info("%s Bug %s  %s", bug_emoji, bug_id, " ".join(parts))

        for w in bug_warnings:
            logger.warning(w)

    return run


def setup_logging(log_dir: Path, *, debug: bool = False) -> logging.Logger:
    logger = logging.getLogger("shadowtree")
    logger.setLevel(logging.DEBUG)

    console = RichHandler(
        level=logging.DEBUG if debug else logging.INFO,
        show_path=False,
        markup=False,
    )
    logger.addHandler(console)

    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fh = logging.FileHandler(log_dir / f"shadowtree_{timestamp}.log")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(fh)

    return logger


if __name__ == "__main__":
    main()
