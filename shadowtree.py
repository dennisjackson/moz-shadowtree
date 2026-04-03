#!/usr/bin/env python3
"""
Clones a repo, looks up Phabricator revisions for each bug in Bugzilla,
checks revision status via Phabricator Conduit, finds the stack tip,
creates a single worktree, and applies each stack via moz-phab patch.
"""

import argparse
import dataclasses
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import requests
import requests_cache
from requests.adapters import HTTPAdapter
from rich.logging import RichHandler
from urllib3.util.retry import Retry

DEFAULT_BRANCH = "main"
BUGZILLA_REST_URL = "https://bugzilla.mozilla.org/rest/bug"
PHABRICATOR_API_URL = "https://phabricator.services.mozilla.com/api/"

# Revision statuses considered "approved"
APPROVED_STATUSES = {"accepted"}
# Revision statuses that should be skipped entirely
SKIP_STATUSES = {"abandoned"}

DEFAULT_CACHE_DB = ".cache/lookups"
DEFAULT_CACHE_TTL = 3600  # 1 hour


# ---------------------------------------------------------------------------
# HTTP session factory
# ---------------------------------------------------------------------------

def create_session(
    *, cache_db: str | None = DEFAULT_CACHE_DB, cache_ttl: int = DEFAULT_CACHE_TTL
) -> requests.Session:
    """Create an HTTP session with connection pooling, retry, and optional disk cache."""
    if cache_db:
        Path(cache_db).parent.mkdir(parents=True, exist_ok=True)
        session = requests_cache.CachedSession(
            cache_db, backend="sqlite", expire_after=cache_ttl,
            allowable_methods=("GET", "POST"),
        )
    else:
        session = requests.Session()

    retry = Retry(total=3, status_forcelist=[429],
                  respect_retry_after_header=True, backoff_factor=1)
    session.mount("https://", HTTPAdapter(max_retries=retry))

    session.cache_hits = 0
    session.cache_misses = 0

    def _track_cache(response, *args, **kwargs):
        if getattr(response, "from_cache", False):
            session.cache_hits += 1
        else:
            session.cache_misses += 1
        return response

    session.hooks["response"].append(_track_cache)
    return session


# ---------------------------------------------------------------------------
# Phabricator Conduit helpers
# ---------------------------------------------------------------------------

def phab_call(method: str, args: dict, api_token: str, session: requests.Session) -> dict:
    """Call a Phabricator Conduit API method and return the result."""
    url = PHABRICATOR_API_URL + method
    data = {
        "params": json.dumps(
            {**args, "__conduit__": {"token": api_token}},
            separators=(",", ":"),
        ),
        "output": "json",
        "__conduit__": True,
    }

    resp = session.post(url, data=data)
    resp.raise_for_status()
    result = resp.json()

    if result.get("error_code"):
        raise RuntimeError(
            f"Phabricator error: {result.get('error_info', result['error_code'])}"
        )
    return result["result"]


def phab_get_revisions(rev_ids: list[int], api_token: str, session: requests.Session) -> list[dict]:
    """Fetch revision metadata (including status) for a list of integer IDs."""
    result = phab_call("differential.revision.search", {
        "constraints": {"ids": rev_ids},
        "attachments": {"reviewers": True},
    }, api_token, session)
    return result.get("data", [])


def _walk_stack_edges(
    phid: str, edge_type: str, api_token: str, session: requests.Session, logger: logging.Logger
) -> list[str]:
    """Walk a single-direction edge chain (parent or child) from *phid*.

    Returns a list of PHIDs in traversal order (immediate neighbour first).
    Stops and logs a warning if the graph branches (non-linear stack).
    """
    result: list[str] = []
    current = phid
    while True:
        edges = phab_call("edge.search", {
            "sourcePHIDs": [current],
            "types": [edge_type],
        }, api_token, session)
        edge_data = edges.get("data", [])
        if not edge_data:
            break
        if len(edge_data) > 1:
            logger.warning(
                "⚠️  Non-linear stack at %s (%d edges), stopping walk",
                current, len(edge_data),
            )
            break
        next_phid = edge_data[0]["destinationPHID"]
        result.append(next_phid)
        current = next_phid
    return result


def phab_find_stack_tip(rev_id: int, api_token: str, session: requests.Session, logger: logging.Logger) -> tuple[int, list[dict]]:
    """Given a revision ID, walk to the top *and* bottom of its stack.

    Returns (tip_rev_id, all_revisions_in_stack) where all_revisions_in_stack
    is ordered bottom-to-top (base first, tip last).
    """
    revs = phab_get_revisions([rev_id], api_token, session)
    if not revs:
        raise RuntimeError(f"Revision D{rev_id} not found on Phabricator")

    base_rev = revs[0]

    # Walk down to the stack base (parents)
    parent_phids = _walk_stack_edges(
        base_rev["phid"], "revision.parent", api_token, session, logger
    )
    # Walk up to the stack tip (children)
    children_phids = _walk_stack_edges(
        base_rev["phid"], "revision.child", api_token, session, logger
    )

    # Fetch full revision data for all related PHIDs
    related_phids = parent_phids + children_phids
    if related_phids:
        related_revs = phab_call("differential.revision.search", {
            "constraints": {"phids": related_phids},
            "attachments": {"reviewers": True},
        }, api_token, session).get("data", [])
        related_map = {r["phid"]: r for r in related_revs}
    else:
        related_map = {}

    # Assemble the full stack: parents (reversed so base is first), self, children
    parent_revs = [related_map[p] for p in reversed(parent_phids) if p in related_map]
    child_revs = [related_map[p] for p in children_phids if p in related_map]
    all_revs = parent_revs + [base_rev] + child_revs

    tip = all_revs[-1]
    tip_id = tip["id"]
    logger.debug(
        "🔗 D%d stack: %s → tip D%d",
        rev_id,
        " → ".join(f"D{r['id']}" for r in all_revs),
        tip_id,
    )
    return tip_id, all_revs


def get_revision_status_emojis(
    revisions: list[dict], logger: logging.Logger
) -> dict[int, str]:
    """Return a mapping of revision ID to a status emoji.

    ✅ = accepted, ⏳ = needs-review/changes-planned, ⚠️ = needs-revision, 🗑️ = abandoned.
    """
    STATUS_EMOJI = {
        "accepted": "✅",
        "needs-review": "⏳",
        "changes-planned": "⏳",
        "needs-revision": "⚠️ ",
        "abandoned": "🗑️",
    }
    result: dict[int, str] = {}
    for rev in revisions:
        status = rev["fields"]["status"]["value"]
        rev_id = rev["id"]
        result[rev_id] = STATUS_EMOJI.get(status, "❓")
        logger.debug("D%d [%s] → %s", rev_id, status, result[rev_id])
    return result


def phab_get_revision_paths(
    revisions: list[dict], api_token: str, session: requests.Session, logger: logging.Logger
) -> dict[int, set[str]]:
    """Fetch the file paths touched by each revision via Phabricator.

    Returns a mapping of revision ID -> set of file paths.
    """
    if not revisions:
        return {}

    rev_phids = [r["phid"] for r in revisions]
    phid_to_id = {r["phid"]: r["id"] for r in revisions}

    # Get the latest diff ID for each revision
    diffs_result = phab_call("differential.diff.search", {
        "constraints": {"revisionPHIDs": rev_phids},
        "order": "newest",
        "limit": len(rev_phids) * 5,
    }, api_token, session)

    rev_phid_to_diff_id: dict[str, int] = {}
    for diff in diffs_result.get("data", []):
        rev_phid = diff["fields"].get("revisionPHID")
        if rev_phid and rev_phid not in rev_phid_to_diff_id:
            rev_phid_to_diff_id[rev_phid] = diff["id"]

    result: dict[int, set[str]] = {}
    for rev_phid, diff_id in rev_phid_to_diff_id.items():
        rev_id = phid_to_id.get(rev_phid)
        if rev_id is None:
            continue
        try:
            raw = phab_call("differential.getrawdiff", {
                "diffID": diff_id,
            }, api_token, session)
            paths = set()
            if isinstance(raw, str):
                for line in raw.splitlines():
                    m = re.match(r"^diff --git a/(.*) b/(.*)$", line)
                    if m:
                        paths.add(m.group(2))
            result[rev_id] = paths
            logger.debug("📄 D%d: %d file(s)", rev_id, len(paths))
        except RuntimeError as e:
            logger.warning("⚠️  Could not fetch diff for D%d: %s", rev_id, e)
            result[rev_id] = set()

    return result


# ---------------------------------------------------------------------------
# Bugzilla
# ---------------------------------------------------------------------------

def get_revisions_for_bug(
    bug_id: str, bz_api_key: str, session: requests.Session, logger: logging.Logger
) -> list[int]:
    """Query Bugzilla for Phabricator revision attachments on a bug.

    Returns a list of integer revision IDs (e.g. [12345, 12346]).
    """
    url = f"{BUGZILLA_REST_URL}/{bug_id}/attachment"
    logger.debug("🔍 Bug %s attachments", bug_id)

    try:
        resp = session.get(url, headers={
            "Accept": "application/json",
            "X-BUGZILLA-API-KEY": bz_api_key,
        })
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.error("❌ Bug %s: %s", bug_id, e)
        return []

    rev_ids: list[int] = []
    for attachment in data.get("bugs", {}).get(str(bug_id), []):
        if attachment.get("content_type") != "text/x-phabricator-request":
            continue

        # Extract the D-number from file_name (e.g. "phabricator-D12345-url.txt")
        # then fall back to the summary field.
        extracted = _extract_revision_id(attachment.get("file_name", ""))
        if extracted is None:
            extracted = _extract_revision_id(attachment.get("summary", ""))
        if extracted is not None:
            rev_ids.append(extracted)
        else:
            logger.warning(
                "⚠️  Bug %s: can't extract rev ID from: %s",
                bug_id,
                attachment.get("file_name") or attachment.get("summary"),
            )

    if rev_ids:
        logger.debug("🐛 Bug %s → %s", bug_id, ", ".join(f"D{r}" for r in rev_ids))
    else:
        logger.debug("Bug %s: no revisions found", bug_id)

    return rev_ids


def _extract_revision_id(text: str) -> int | None:
    """Extract a Phabricator revision integer ID from text like 'D12345'."""
    for part in text.replace("-", " ").replace("_", " ").split():
        if part.startswith("D") and part[1:].isdigit():
            return int(part[1:])
    return None


# ---------------------------------------------------------------------------
# Git / worktree helpers
# ---------------------------------------------------------------------------

def _run_git(args: list[str], logger: logging.Logger, **kwargs) -> subprocess.CompletedProcess:
    """Run a git command, capturing output and logging it at debug level."""
    check = kwargs.pop("check", False)
    result = subprocess.run(args, capture_output=True, text=True, **kwargs)
    if result.stdout.strip():
        logger.debug("%s", result.stdout.strip())
    if result.stderr.strip():
        logger.debug("%s", result.stderr.strip())
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, args)
    return result


def clone_repo(dest: Path, repo_url: str, branch: str, logger: logging.Logger,
               *, tag: str | None = None) -> None:
    if (dest / ".git").exists():
        logger.debug("📦 Updating repo at %s", dest)
        _run_git(["git", "fetch", "origin"], logger, cwd=dest, check=True)
        if tag:
            _run_git(["git", "checkout", tag], logger, cwd=dest, check=True)
        else:
            _run_git(["git", "checkout", branch], logger, cwd=dest, check=True)
            _run_git(["git", "reset", "--hard", f"origin/{branch}"], logger, cwd=dest, check=True)
    else:
        logger.debug("📦 Cloning %s → %s", repo_url, dest)
        _run_git(["git", "clone", repo_url, str(dest)], logger, check=True)


def _cleanup_old_branches(repo: Path, logger: logging.Logger) -> None:
    """Delete any leftover patch-apply-* branches from previous runs."""
    result = _run_git(
        ["git", "branch", "--list", "patch-apply-*"],
        logger, cwd=repo,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return
    for line in result.stdout.strip().splitlines():
        branch = line.strip().lstrip("* ")
        logger.debug("🧹 Deleting old branch %s", branch)
        _run_git(["git", "branch", "-D", branch], logger, cwd=repo)


def create_worktree(repo: Path, worktree: Path, branch: str, logger: logging.Logger,
                    *, tag: str | None = None) -> None:
    branch_name = f"patch-apply-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    start_point = tag if tag else f"origin/{branch}"
    if worktree.exists():
        logger.debug("🌿 Removing old worktree %s", worktree)
        result = _run_git(
            ["git", "worktree", "remove", "--force", str(worktree)],
            logger, cwd=repo,
        )
        if result.returncode != 0 and worktree.exists():
            logger.debug("🌿 git worktree remove failed; falling back to rmtree")
            shutil.rmtree(worktree)
    _cleanup_old_branches(repo, logger)
    logger.debug("🌿 Worktree %s [%s] from %s", worktree, branch_name, start_point)
    _run_git(
        ["git", "worktree", "add", "-b", branch_name, str(worktree), start_point],
        logger, cwd=repo, check=True,
    )


# ---------------------------------------------------------------------------
# Patch application
# ---------------------------------------------------------------------------

def verify_worktree_clean(worktree: Path, logger: logging.Logger) -> bool:
    """Return True if the worktree has no uncommitted changes or untracked files."""
    result = _run_git(["git", "status", "--porcelain"], logger, cwd=worktree)
    if result.returncode != 0:
        logger.error("❌ git status failed: %s", result.stderr)
        return False
    if result.stdout.strip():
        logger.error("❌ Worktree dirty, aborting:\n%s", result.stdout.strip())
        return False
    return True


def get_conflict_files(worktree: Path, logger: logging.Logger) -> set[str]:
    """Detect files with conflicts or dirty state in the worktree after a failed patch."""
    files: set[str] = set()
    for diff_filter in ["--diff-filter=U", None]:
        args = ["git", "diff", "--name-only"]
        if diff_filter:
            args.append(diff_filter)
        result = _run_git(args, logger, cwd=worktree)
        if result.returncode == 0 and result.stdout.strip():
            files.update(result.stdout.strip().splitlines())
    return files


def diagnose_conflict(
    conflict_files: set[str],
    stack_file_map: dict[int, set[str]],
    applied_files: dict[str, list[tuple[str, int]]],
    logger: logging.Logger,
) -> None:
    """Log diagnostic information about which revisions likely caused a conflict."""
    # All files this stack touches
    all_stack_files: set[str] = set()
    for paths in stack_file_map.values():
        all_stack_files |= paths

    if conflict_files:
        logger.error("💥 Conflicting files: %s", ", ".join(sorted(conflict_files)))

    # Find files that overlap between this stack and previously applied patches
    candidate_files: set[str] = set()
    prior_hits: dict[tuple[str, int], list[str]] = {}
    for f in all_stack_files:
        for key in applied_files.get(f, []):
            candidate_files.add(f)
            prior_hits.setdefault(key, []).append(f)

    if candidate_files:
        logger.error(
            "🔀 Candidate clashes with prior patches: %s",
            ", ".join(sorted(candidate_files)),
        )
        # Which revisions in this stack touch the candidate files?
        for rev_id in sorted(stack_file_map):
            overlap = sorted(candidate_files & stack_file_map[rev_id])
            if overlap:
                logger.error("   D%d: %s", rev_id, ", ".join(overlap))
        # Which prior patches touched them?
        for (bug_id, tip_id), files in sorted(prior_hits.items()):
            logger.error(
                "   ↳ Bug %s (D%d): %s", bug_id, tip_id, ", ".join(sorted(files))
            )
    else:
        logger.error("🌲 No overlap with prior patches — likely conflicts with base tree")


def apply_stack(
    tip_rev_id: int,
    worktree: Path,
    logger: logging.Logger,
    *,
    stack_file_map: dict[int, set[str]] | None = None,
    applied_files: dict[str, list[tuple[str, int]]] | None = None,
) -> tuple[bool, set[str]]:
    """Apply the full stack ending at tip_rev_id using moz-phab patch.

    moz-phab will automatically fetch and apply the entire ancestor chain.
    Returns (success, set_of_files_modified).
    """
    revision_str = f"D{tip_rev_id}"

    # Verify clean state before applying
    if not verify_worktree_clean(worktree, logger):
        return False, set()

    # Record HEAD so we can reset to it on failure
    head_result = _run_git(["git", "rev-parse", "HEAD"], logger, cwd=worktree)
    if head_result.returncode != 0:
        logger.error("❌ Can't determine HEAD in %s", worktree)
        return False, set()
    pre_patch_head = head_result.stdout.strip()

    logger.debug("📎 Applying %s", revision_str)

    result = subprocess.run(
        ["moz-phab", "patch", "--apply-to", "here", "--yes", revision_str],
        cwd=worktree, capture_output=True, text=True,
    )

    if result.returncode != 0:
        logger.error("❌ %s failed (exit %d)", revision_str, result.returncode)
        logger.debug("stdout:\n%s", result.stdout)
        logger.debug("stderr:\n%s", result.stderr)

        # Diagnose the conflict before resetting
        if stack_file_map is not None:
            conflict_files = get_conflict_files(worktree, logger)
            diagnose_conflict(
                conflict_files, stack_file_map, applied_files or {}, logger,
            )

        # Reset to pre-patch state
        _run_git(["git", "reset", "--hard", pre_patch_head], logger, cwd=worktree)
        _run_git(["git", "clean", "-fd"], logger, cwd=worktree)
        return False, set()

    # Record which files this stack modified
    diff_result = _run_git(
        ["git", "diff", "--name-only", f"{pre_patch_head}..HEAD"],
        logger, cwd=worktree,
    )
    modified_files: set[str] = set()
    if diff_result.returncode == 0 and diff_result.stdout.strip():
        modified_files = set(diff_result.stdout.strip().splitlines())

    logger.debug("moz-phab stdout:\n%s", result.stdout)
    if result.stderr:
        logger.debug("moz-phab stderr:\n%s", result.stderr)
    logger.debug("✅ %s applied (%d files)", revision_str, len(modified_files))
    return True, modified_files


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def parse_bug_file(bug_file: Path) -> tuple[str, str, str | None, str, list[str]]:
    """Parse a bug list file, extracting optional repo/branch/tag/name directives.

    Supports directives as comments:
        # name: nss-sec-high
        # repo: https://github.com/nss-dev/nss.git
        # branch: main
        # tag: NSS_3_99_RTM
    """
    directives: dict[str, str] = {}
    bug_ids: list[str] = []

    for line in bug_file.read_text().splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            comment = stripped.lstrip("# ").strip()
            key, _, value = comment.partition(":")
            if key.lower() in {"repo", "branch", "tag", "name"} and value:
                directives[key.lower()] = value.strip()
        else:
            bug_ids.append(stripped)

    if "tag" in directives and "branch" in directives:
        raise ValueError("Bug file specifies both 'branch' and 'tag' — use one or the other")
    if "repo" not in directives:
        raise ValueError("Bug file must include a '# repo: <url>' directive")

    return (
        directives["repo"],
        directives.get("branch", DEFAULT_BRANCH),
        directives.get("tag"),
        directives.get("name", bug_file.stem),
        bug_ids,
    )


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

    checks = [
        (shutil.which("moz-phab"), "moz-phab not found on PATH"),
        (shutil.which("git"), "git not found on PATH"),
        (bz_api_key, "BUGZILLA_API_KEY not set"),
        (phab_api_token, "PHABRICATOR_API_TOKEN not set"),
        (bug_file.exists(), f"Bug file not found: {bug_file}"),
    ]
    for val, msg in checks:
        if not val:
            logger.error("❌ %s", msg)
    if not all(v for v, _ in checks):
        sys.exit(1)

    repo_url, branch, tag, name, bug_ids = parse_bug_file(bug_file)
    logger.info("📋 %d bug(s) from %s (name: %s)", len(bug_ids), bug_file, name)
    if tag:
        logger.info("🔗 Repo: %s  Tag: %s", repo_url, tag)
    else:
        logger.info("🔗 Repo: %s  Branch: %s", repo_url, branch)

    # Clone / update repo
    out_dir = args.out_dir.resolve()
    repo_name = repo_url.rstrip("/").rsplit("/", 1)[-1].removesuffix(".git")
    repo_dir = out_dir / repo_name
    clone_repo(repo_dir, repo_url, branch, logger, tag=tag)

    # Create a single worktree
    worktree_dir = out_dir / name
    create_worktree(repo_dir, worktree_dir, branch, logger, tag=tag)

    # For each bug: find revisions, resolve stacks, check statuses, apply
    session = create_session(
        cache_db=None if args.no_cache else DEFAULT_CACHE_DB,
        cache_ttl=args.cache_ttl,
    )
    try:
        run = _main_loop(bug_ids, bz_api_key, phab_api_token, session,
                         worktree_dir, logger)
    finally:
        session.close()

    logger.info(
        "🗄️  Cache: %d hits / %d misses",
        session.cache_hits, session.cache_misses,
    )

    # Summary
    logger.info(
        "── %d applied · %d failed · %d no patches ──",
        len(run.succeeded), len(run.failed), len(run.skipped_bugs),
    )
    logger.info("📂 Worktree: %s", worktree_dir)


def _main_loop(
    bug_ids: list[str],
    bz_api_key: str,
    phab_api_token: str,
    session: requests.Session,
    worktree_dir: Path,
    logger: logging.Logger,
) -> RunResult:
    run = RunResult()

    for bug_id in bug_ids:
        logger.debug("── Bug %s ──", bug_id)

        # Step 1: Get revision IDs from Bugzilla
        rev_ids = get_revisions_for_bug(bug_id, bz_api_key, session, logger)
        if not rev_ids:
            run.skipped_bugs.append(bug_id)
            logger.info("📭 Bug %s", bug_id)
            continue

        # Step 2: For each revision, find the stack tip and check statuses
        # Deduplicate tips — multiple revisions on one bug may share a stack
        tips_seen: set[int] = set()
        # Per-revision status emoji and per-tip apply result
        rev_emojis: dict[int, str] = {}
        tip_applied: dict[int, bool] = {}
        # Warnings to print after the summary line (only for real failures)
        bug_warnings: list[str] = []

        for rev_id in rev_ids:
            try:
                tip_id, stack_revs = phab_find_stack_tip(
                    rev_id, phab_api_token, session, logger
                )
            except RuntimeError as e:
                run.failed.append((bug_id, rev_id, str(e)))
                rev_emojis[rev_id] = "❌"
                tip_applied[rev_id] = False
                bug_warnings.append(f"  D{rev_id}: {e}")
                continue

            # Skip if the entry revision itself is abandoned
            entry_rev = next(
                (r for r in stack_revs if r["id"] == rev_id), None
            )
            if entry_rev and entry_rev["fields"]["status"]["value"] in SKIP_STATUSES:
                logger.debug("🗑️  D%d abandoned, skipping", rev_id)
                continue

            # Collect per-revision status emojis
            rev_emojis.update(get_revision_status_emojis(stack_revs, logger))

            if tip_id in tips_seen:
                logger.debug("⏭️  D%d (tip D%d already queued)", rev_id, tip_id)
                continue
            tips_seen.add(tip_id)

            # Fetch which files each revision in the stack touches
            stack_file_map = phab_get_revision_paths(
                stack_revs, phab_api_token, session, logger
            )

            # Step 3: Apply the stack via moz-phab
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

        # One-line per-bug summary
        # Each revision gets its status emoji; failed applies override with ❌
        parts: list[str] = []
        all_good = True
        for rev_id, emoji in rev_emojis.items():
            if rev_id in tip_applied and not tip_applied[rev_id]:
                emoji = "❌"
            if emoji != "✅":
                all_good = False
            parts.append(f"D{rev_id}{emoji}")
        bug_emoji = "✅" if all_good else "⚠️ "
        logger.info("%s Bug %s  %s", bug_emoji, bug_id, " ".join(parts))

        # Print warnings for actual failures only
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
