#!/usr/bin/env python3
"""
Clones a repo, looks up Phabricator revisions for each bug in Bugzilla,
checks revision status via Phabricator Conduit, finds the stack tip,
creates a single worktree, and applies each stack via moz-phab patch.
"""

import argparse
import hashlib
import http.client
import json
import logging
import os
import re
import shutil
import sqlite3
import ssl
import subprocess
import sys
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

DEFAULT_BRANCH = "master"
BUGZILLA_REST_URL = "https://bugzilla.mozilla.org/rest/bug"
PHABRICATOR_API_URL = "https://phabricator.services.mozilla.com/api/"

# Revision statuses considered "approved"
APPROVED_STATUSES = {"accepted"}
# Revision statuses that should be skipped entirely
SKIP_STATUSES = {"abandoned"}

DEFAULT_CACHE_DB = Path(".cache/lookups.db")
DEFAULT_CACHE_TTL = 3600  # 1 hour


# ---------------------------------------------------------------------------
# Persistent HTTP connection pool (stdlib only)
# ---------------------------------------------------------------------------

class _ConnectionPool:
    """Reuses HTTP(S) connections per (host, port) pair with optional disk cache."""

    def __init__(self, timeout: int = 30,
                 cache_db: Path | None = DEFAULT_CACHE_DB,
                 cache_ttl: int = DEFAULT_CACHE_TTL):
        self._timeout = timeout
        self._conns: dict[tuple[str, int], http.client.HTTPSConnection] = {}
        self._db: sqlite3.Connection | None = None
        self._cache_ttl = cache_ttl
        self.cache_hits = 0
        self.cache_misses = 0
        if cache_db is not None:
            cache_db.parent.mkdir(parents=True, exist_ok=True)
            self._db = sqlite3.connect(str(cache_db))
            self._db.execute(
                "CREATE TABLE IF NOT EXISTS cache "
                "(key TEXT PRIMARY KEY, body BLOB, ts REAL)"
            )
            self._db.execute(
                "DELETE FROM cache WHERE ts < ?",
                (time.time() - cache_ttl,),
            )
            self._db.commit()

    @staticmethod
    def _cache_key(url: str, data: bytes | None) -> str:
        raw = url.encode() + b"\0" + (data or b"")
        return hashlib.sha256(raw).hexdigest()

    def _cache_get(self, key: str) -> bytes | None:
        if self._db is None:
            return None
        row = self._db.execute(
            "SELECT body FROM cache WHERE key = ? AND ts >= ?",
            (key, time.time() - self._cache_ttl),
        ).fetchone()
        return row[0] if row else None

    def _cache_put(self, key: str, body: bytes) -> None:
        if self._db is None:
            return
        self._db.execute(
            "INSERT OR REPLACE INTO cache (key, body, ts) VALUES (?, ?, ?)",
            (key, body, time.time()),
        )
        self._db.commit()

    def urlopen(self, url: str, data: bytes | None = None,
                headers: dict[str, str] | None = None,
                method: str = "POST") -> bytes:
        cache_key = self._cache_key(url, data)
        cached = self._cache_get(cache_key)
        if cached is not None:
            self.cache_hits += 1
            return cached
        self.cache_misses += 1

        parsed = urllib.parse.urlparse(url)
        host = parsed.hostname
        port = parsed.port or 443
        key = (host, port)

        path = parsed.path
        if parsed.query:
            path = f"{path}?{parsed.query}"

        hdrs = headers or {}

        while True:
            conn = self._conns.get(key)
            if conn is None:
                ctx = ssl.create_default_context()
                conn = http.client.HTTPSConnection(host, port, timeout=self._timeout,
                                                   context=ctx)
                self._conns[key] = conn

            try:
                conn.request(method, path, body=data, headers=hdrs)
                resp = conn.getresponse()
            except (http.client.RemoteDisconnected, ConnectionError, OSError):
                # Server closed the keep-alive connection; reconnect once.
                ctx = ssl.create_default_context()
                conn = http.client.HTTPSConnection(host, port, timeout=self._timeout,
                                                   context=ctx)
                self._conns[key] = conn
                conn.request(method, path, body=data, headers=hdrs)
                resp = conn.getresponse()

            body = resp.read()

            if resp.status == 429:
                delay = int(resp.getheader("Retry-After", "5"))
                logging.getLogger("shadowtree").warning(
                    "⏳ Rate-limited by %s, retrying in %ds", host, delay,
                )
                time.sleep(delay)
                continue

            if resp.status >= 400:
                raise http.client.HTTPException(
                    f"HTTP {resp.status} {resp.reason} for {url}"
                )

            self._cache_put(cache_key, body)
            return body

    def close(self) -> None:
        for conn in self._conns.values():
            conn.close()
        self._conns.clear()
        if self._db is not None:
            self._db.close()


# ---------------------------------------------------------------------------
# Phabricator Conduit helpers
# ---------------------------------------------------------------------------

def phab_call(method: str, args: dict, api_token: str, pool: _ConnectionPool) -> dict:
    """Call a Phabricator Conduit API method and return the result."""
    url = urllib.parse.urljoin(PHABRICATOR_API_URL, method)
    payload = urllib.parse.urlencode({
        "params": json.dumps(
            {**args, "__conduit__": {"token": api_token}},
            separators=(",", ":"),
        ),
        "output": "json",
        "__conduit__": True,
    }).encode()

    body = pool.urlopen(url, data=payload, headers={
        "Content-Type": "application/x-www-form-urlencoded",
    })
    data = json.loads(body)

    if data.get("error_code"):
        raise RuntimeError(
            f"Phabricator error: {data.get('error_info', data['error_code'])}"
        )
    return data["result"]


def phab_get_revisions(rev_ids: list[int], api_token: str, pool: _ConnectionPool) -> list[dict]:
    """Fetch revision metadata (including status) for a list of integer IDs."""
    result = phab_call("differential.revision.search", {
        "constraints": {"ids": rev_ids},
        "attachments": {"reviewers": True},
    }, api_token, pool)
    return result.get("data", [])


def _walk_stack_edges(
    phid: str, edge_type: str, api_token: str, pool: _ConnectionPool, logger: logging.Logger
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
        }, api_token, pool)
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


def phab_find_stack_tip(rev_id: int, api_token: str, pool: _ConnectionPool, logger: logging.Logger) -> tuple[int, list[dict]]:
    """Given a revision ID, walk to the top *and* bottom of its stack.

    Returns (tip_rev_id, all_revisions_in_stack) where all_revisions_in_stack
    is ordered bottom-to-top (base first, tip last).
    """
    revs = phab_get_revisions([rev_id], api_token, pool)
    if not revs:
        raise RuntimeError(f"Revision D{rev_id} not found on Phabricator")

    base_rev = revs[0]

    # Walk down to the stack base (parents)
    parent_phids = _walk_stack_edges(
        base_rev["phid"], "revision.parent", api_token, pool, logger
    )
    # Walk up to the stack tip (children)
    children_phids = _walk_stack_edges(
        base_rev["phid"], "revision.child", api_token, pool, logger
    )

    # Fetch full revision data for all related PHIDs
    related_phids = parent_phids + children_phids
    if related_phids:
        related_revs = phab_call("differential.revision.search", {
            "constraints": {"phids": related_phids},
            "attachments": {"reviewers": True},
        }, api_token, pool).get("data", [])
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
    revisions: list[dict], api_token: str, pool: _ConnectionPool, logger: logging.Logger
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
    }, api_token, pool)

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
            }, api_token, pool)
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
    bug_id: str, bz_api_key: str, pool: _ConnectionPool, logger: logging.Logger
) -> list[int]:
    """Query Bugzilla for Phabricator revision attachments on a bug.

    Returns a list of integer revision IDs (e.g. [12345, 12346]).
    """
    url = f"{BUGZILLA_REST_URL}/{bug_id}/attachment"
    logger.debug("🔍 Bug %s attachments", bug_id)

    try:
        raw = pool.urlopen(url, method="GET", headers={
            "Accept": "application/json",
            "X-BUGZILLA-API-KEY": bz_api_key,
        })
        data = json.loads(raw)
    except (http.client.HTTPException, ConnectionError, OSError) as e:
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
    result = subprocess.run(args, capture_output=True, text=True, **kwargs)
    if result.stdout.strip():
        logger.debug("%s", result.stdout.strip())
    if result.stderr.strip():
        logger.debug("%s", result.stderr.strip())
    if kwargs.get("check", False) and result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, args)
    return result


def clone_repo(dest: Path, repo_url: str, branch: str, logger: logging.Logger,
               *, tag: str | None = None) -> None:
    ref = tag or branch
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
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=worktree,
        capture_output=True,
        text=True,
    )
    if status.returncode != 0:
        logger.error("❌ git status failed: %s", status.stderr)
        return False
    if status.stdout.strip():
        logger.error("❌ Worktree dirty, aborting:\n%s", status.stdout.strip())
        return False
    return True


def get_conflict_files(worktree: Path, logger: logging.Logger) -> set[str]:
    """Detect files with conflicts or dirty state in the worktree after a failed patch."""
    files: set[str] = set()
    # Unmerged paths (actual merge conflicts)
    result = subprocess.run(
        ["git", "diff", "--name-only", "--diff-filter=U"],
        cwd=worktree, capture_output=True, text=True,
    )
    if result.returncode == 0 and result.stdout.strip():
        files.update(result.stdout.strip().splitlines())
    # Any other dirty files (failed applies that left partial changes)
    result = subprocess.run(
        ["git", "diff", "--name-only"],
        cwd=worktree, capture_output=True, text=True,
    )
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
    head_result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=worktree,
        capture_output=True,
        text=True,
    )
    if head_result.returncode != 0:
        logger.error("❌ Can't determine HEAD in %s", worktree)
        return False, set()
    pre_patch_head = head_result.stdout.strip()

    logger.debug("📎 Applying %s", revision_str)

    result = subprocess.run(
        [
            "moz-phab", "patch",
            "--apply-to", "here",
            "--yes",
            revision_str,
        ],
        cwd=worktree,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        logger.error("❌ %s failed (exit %d)", revision_str, result.returncode)
        logger.debug("stdout:\n%s", result.stdout)
        logger.debug("stderr:\n%s", result.stderr)

        # Diagnose the conflict before resetting
        if stack_file_map is not None:
            conflict_files = get_conflict_files(worktree, logger)
            diagnose_conflict(
                conflict_files,
                stack_file_map,
                applied_files or {},
                logger,
            )

        # Reset to pre-patch state
        subprocess.run(
            ["git", "reset", "--hard", pre_patch_head],
            cwd=worktree, check=False,
        )
        subprocess.run(["git", "clean", "-fd"], cwd=worktree, check=False)
        return False, set()

    # Record which files this stack modified
    diff_result = subprocess.run(
        ["git", "diff", "--name-only", f"{pre_patch_head}..HEAD"],
        cwd=worktree, capture_output=True, text=True,
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
    repo_url: str | None = None
    name: str | None = None
    branch = DEFAULT_BRANCH
    tag: str | None = None
    branch_set = False
    bug_ids: list[str] = []

    for line in bug_file.read_text().splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            comment = stripped.lstrip("# ").strip()
            if comment.lower().startswith("repo:"):
                repo_url = comment.split(":", 1)[1].strip()
            elif comment.lower().startswith("branch:"):
                branch = comment.split(":", 1)[1].strip()
                branch_set = True
            elif comment.lower().startswith("tag:"):
                tag = comment.split(":", 1)[1].strip()
            elif comment.lower().startswith("name:"):
                name = comment.split(":", 1)[1].strip()
        else:
            bug_ids.append(stripped)

    if tag and branch_set:
        raise ValueError("Bug file specifies both 'branch' and 'tag' — use one or the other")
    if repo_url is None:
        raise ValueError("Bug file must include a '# repo: <url>' directive")
    if name is None:
        name = bug_file.stem

    return repo_url, branch, tag, name, bug_ids


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
    ok = True

    if not shutil.which("moz-phab"):
        logger.error("❌ moz-phab not found on PATH")
        ok = False

    if not shutil.which("git"):
        logger.error("❌ git not found on PATH")
        ok = False

    bz_api_key = os.environ.get("BUGZILLA_API_KEY")
    if not bz_api_key:
        logger.error("❌ BUGZILLA_API_KEY not set")
        ok = False

    phab_api_token = os.environ.get("PHABRICATOR_API_TOKEN")
    if not phab_api_token:
        logger.error("❌ PHABRICATOR_API_TOKEN not set")
        ok = False

    bug_file: Path = args.bug_file
    if not bug_file.exists():
        logger.error("❌ Bug file not found: %s", bug_file)
        ok = False

    if not ok:
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
    succeeded: list[tuple[str, int]] = []
    failed: list[tuple[str, int, str]] = []
    skipped_bugs: list[str] = []
    # Track which files have been modified by successfully-applied stacks,
    # so we can identify the source of conflicts when a later patch fails.
    applied_files: dict[str, list[tuple[str, int]]] = {}

    pool = _ConnectionPool(
        cache_db=None if args.no_cache else DEFAULT_CACHE_DB,
        cache_ttl=args.cache_ttl,
    )
    try:
        _main_loop(bug_ids, bz_api_key, phab_api_token, pool, worktree_dir, logger,
                   succeeded, failed, skipped_bugs, applied_files)
    finally:
        pool.close()

    logger.info(
        "🗄️  Cache: %d hits / %d misses",
        pool.cache_hits, pool.cache_misses,
    )

    # Summary
    logger.info(
        "── %d applied · %d failed · %d no patches ──",
        len(succeeded), len(failed), len(skipped_bugs),
    )
    logger.info("📂 Worktree: %s", worktree_dir)


def _main_loop(
    bug_ids: list[str],
    bz_api_key: str,
    phab_api_token: str,
    pool: _ConnectionPool,
    worktree_dir: Path,
    logger: logging.Logger,
    succeeded: list[tuple[str, int]],
    failed: list[tuple[str, int, str]],
    skipped_bugs: list[str],
    applied_files: dict[str, list[tuple[str, int]]],
) -> None:
    for bug_id in bug_ids:
        logger.debug("── Bug %s ──", bug_id)

        # Step 1: Get revision IDs from Bugzilla
        rev_ids = get_revisions_for_bug(bug_id, bz_api_key, pool, logger)
        if not rev_ids:
            skipped_bugs.append(bug_id)
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
                    rev_id, phab_api_token, pool, logger
                )
            except RuntimeError as e:
                failed.append((bug_id, rev_id, str(e)))
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
                stack_revs, phab_api_token, pool, logger
            )

            # Step 3: Apply the stack via moz-phab
            ok, modified_files = apply_stack(
                tip_id, worktree_dir, logger,
                stack_file_map=stack_file_map,
                applied_files=applied_files,
            )
            tip_applied[tip_id] = ok
            if ok:
                succeeded.append((bug_id, tip_id))
                for f in modified_files:
                    applied_files.setdefault(f, []).append((bug_id, tip_id))
            else:
                failed.append((bug_id, tip_id, "patch failed"))
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


class _ColorFormatter(logging.Formatter):
    """Logging formatter that adds ANSI colour to the level name."""

    COLORS = {
        logging.DEBUG: "\033[36m",     # cyan
        logging.INFO: "\033[32m",      # green
        logging.WARNING: "\033[33m",   # yellow
        logging.ERROR: "\033[31m",     # red
        logging.CRITICAL: "\033[1;31m",  # bold red
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        color = self.COLORS.get(record.levelno, "")
        record.levelname = f"{color}{record.levelname}{self.RESET}"
        return super().format(record)


def setup_logging(log_dir: Path, *, debug: bool = False) -> logging.Logger:
    logger = logging.getLogger("shadowtree")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if debug else logging.INFO)
    if sys.stdout.isatty():
        console.setFormatter(_ColorFormatter("%(asctime)s [%(levelname)s] %(message)s"))
    else:
        console.setFormatter(formatter)
    logger.addHandler(console)

    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fh = logging.FileHandler(log_dir / f"shadowtree_{timestamp}.log")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger


if __name__ == "__main__":
    main()
