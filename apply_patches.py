#!/usr/bin/env python3
"""
Clones NSS, looks up Phabricator revisions for each bug in Bugzilla,
checks revision status via Phabricator Conduit, finds the stack tip,
creates a single worktree, and applies each stack via moz-phab patch.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path

NSS_REPO_URL = "https://github.com/nss-dev/nss.git"
BUGZILLA_REST_URL = "https://bugzilla.mozilla.org/rest/bug"
PHABRICATOR_API_URL = "https://phabricator.services.mozilla.com/api/"

# Revision statuses considered "approved"
APPROVED_STATUSES = {"accepted"}


# ---------------------------------------------------------------------------
# Phabricator Conduit helpers
# ---------------------------------------------------------------------------

def phab_call(method: str, args: dict, api_token: str) -> dict:
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

    req = urllib.request.Request(url, data=payload, method="POST")
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())

    if data.get("error_code"):
        raise RuntimeError(
            f"Phabricator error: {data.get('error_info', data['error_code'])}"
        )
    return data["result"]


def phab_get_revisions(rev_ids: list[int], api_token: str) -> list[dict]:
    """Fetch revision metadata (including status) for a list of integer IDs."""
    result = phab_call("differential.revision.search", {
        "constraints": {"ids": rev_ids},
        "attachments": {"reviewers": True},
    }, api_token)
    return result.get("data", [])


def _walk_stack_edges(
    phid: str, edge_type: str, api_token: str, logger: logging.Logger
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
        }, api_token)
        edge_data = edges.get("data", [])
        if not edge_data:
            break
        if len(edge_data) > 1:
            logger.warning(
                "  Non-linear stack detected at %s (%s has %d edges), "
                "stopping walk",
                current, edge_type, len(edge_data),
            )
            break
        next_phid = edge_data[0]["destinationPHID"]
        result.append(next_phid)
        current = next_phid
    return result


def phab_find_stack_tip(rev_id: int, api_token: str, logger: logging.Logger) -> tuple[int, list[dict]]:
    """Given a revision ID, walk to the top *and* bottom of its stack.

    Returns (tip_rev_id, all_revisions_in_stack) where all_revisions_in_stack
    is ordered bottom-to-top (base first, tip last).
    """
    revs = phab_get_revisions([rev_id], api_token)
    if not revs:
        raise RuntimeError(f"Revision D{rev_id} not found on Phabricator")

    base_rev = revs[0]

    # Walk down to the stack base (parents)
    parent_phids = _walk_stack_edges(
        base_rev["phid"], "revision.parent", api_token, logger
    )
    # Walk up to the stack tip (children)
    children_phids = _walk_stack_edges(
        base_rev["phid"], "revision.child", api_token, logger
    )

    # Fetch full revision data for all related PHIDs
    related_phids = parent_phids + children_phids
    if related_phids:
        related_revs = phab_call("differential.revision.search", {
            "constraints": {"phids": related_phids},
            "attachments": {"reviewers": True},
        }, api_token).get("data", [])
        related_map = {r["phid"]: r for r in related_revs}
    else:
        related_map = {}

    # Assemble the full stack: parents (reversed so base is first), self, children
    parent_revs = [related_map[p] for p in reversed(parent_phids) if p in related_map]
    child_revs = [related_map[p] for p in children_phids if p in related_map]
    all_revs = parent_revs + [base_rev] + child_revs

    tip = all_revs[-1]
    tip_id = tip["id"]
    logger.info(
        "  D%d stack: %s (tip: D%d)",
        rev_id,
        " -> ".join(f"D{r['id']}" for r in all_revs),
        tip_id,
    )
    return tip_id, all_revs


def check_revision_statuses(
    revisions: list[dict], bug_id: str, logger: logging.Logger
) -> None:
    """Log warnings for any revisions that are not approved."""
    for rev in revisions:
        status = rev["fields"]["status"]["value"]
        status_name = rev["fields"]["status"].get("name", status)
        rev_id = rev["id"]
        if status not in APPROVED_STATUSES:
            logger.warning(
                "  Bug %s: D%d has status '%s' (not approved)", bug_id, rev_id, status_name
            )
        else:
            logger.info("  Bug %s: D%d status '%s'", bug_id, rev_id, status_name)


# ---------------------------------------------------------------------------
# Bugzilla
# ---------------------------------------------------------------------------

def get_revisions_for_bug(
    bug_id: str, bz_api_key: str, logger: logging.Logger
) -> list[int]:
    """Query Bugzilla for Phabricator revision attachments on a bug.

    Returns a list of integer revision IDs (e.g. [12345, 12346]).
    """
    url = f"{BUGZILLA_REST_URL}/{bug_id}/attachment"
    logger.debug("Fetching attachments for bug %s", bug_id)

    req = urllib.request.Request(url)
    req.add_header("Accept", "application/json")
    req.add_header("X-BUGZILLA-API-KEY", bz_api_key)

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        logger.error("Failed to fetch bug %s: HTTP %s", bug_id, e.code)
        return []
    except urllib.error.URLError as e:
        logger.error("Failed to fetch bug %s: %s", bug_id, e.reason)
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
                "Bug %s: could not extract revision ID from attachment: %s",
                bug_id,
                attachment.get("file_name") or attachment.get("summary"),
            )

    if rev_ids:
        logger.info("Bug %s: found revisions %s", bug_id,
                     ", ".join(f"D{r}" for r in rev_ids))
    else:
        logger.warning("Bug %s: no Phabricator revisions found", bug_id)

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

def clone_nss(dest: Path, logger: logging.Logger) -> None:
    if (dest / ".git").exists():
        logger.info("NSS repo already exists at %s, fetching latest", dest)
        subprocess.run(["git", "fetch", "origin"], cwd=dest, check=True)
        subprocess.run(["git", "checkout", "master"], cwd=dest, check=True)
        subprocess.run(
            ["git", "reset", "--hard", "origin/master"], cwd=dest, check=True
        )
    else:
        logger.info("Cloning NSS from %s into %s", NSS_REPO_URL, dest)
        subprocess.run(["git", "clone", NSS_REPO_URL, str(dest)], check=True)


def create_worktree(repo: Path, worktree: Path, logger: logging.Logger) -> None:
    branch_name = f"patch-apply-{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    if worktree.exists():
        logger.info("Removing existing worktree at %s", worktree)
        subprocess.run(
            ["git", "worktree", "remove", "--force", str(worktree)],
            cwd=repo,
            check=False,
        )
    logger.info("Creating worktree at %s on branch %s", worktree, branch_name)
    subprocess.run(
        ["git", "worktree", "add", "-b", branch_name, str(worktree), "origin/master"],
        cwd=repo,
        check=True,
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
        logger.error("git status failed in %s: %s", worktree, status.stderr)
        return False
    if status.stdout.strip():
        logger.error(
            "Worktree %s is not clean, aborting patch:\n%s",
            worktree, status.stdout.strip(),
        )
        return False
    return True


def apply_stack(
    tip_rev_id: int, worktree: Path, logger: logging.Logger
) -> bool:
    """Apply the full stack ending at tip_rev_id using moz-phab patch.

    moz-phab will automatically fetch and apply the entire ancestor chain.
    """
    revision_str = f"D{tip_rev_id}"

    # Verify clean state before applying
    if not verify_worktree_clean(worktree, logger):
        return False

    # Record HEAD so we can reset to it on failure
    head_result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=worktree,
        capture_output=True,
        text=True,
    )
    if head_result.returncode != 0:
        logger.error("Failed to determine HEAD in %s", worktree)
        return False
    pre_patch_head = head_result.stdout.strip()

    logger.info("Applying stack tip %s via moz-phab in %s", revision_str, worktree)

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
        logger.error(
            "Failed to apply %s (exit code %d)", revision_str, result.returncode
        )
        logger.error("stdout:\n%s", result.stdout)
        logger.error("stderr:\n%s", result.stderr)
        # Reset to pre-patch state (undoes partial commits and working tree changes)
        subprocess.run(
            ["git", "reset", "--hard", pre_patch_head],
            cwd=worktree, check=False,
        )
        subprocess.run(["git", "clean", "-fd"], cwd=worktree, check=False)
        return False

    logger.debug("moz-phab stdout:\n%s", result.stdout)
    if result.stderr:
        logger.debug("moz-phab stderr:\n%s", result.stderr)
    logger.info("Successfully applied stack tip %s", revision_str)
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Apply NSS Phabricator patches by bug number.",
    )
    parser.add_argument(
        "bug_file", type=Path, help="File with one bug number per line"
    )
    parser.add_argument(
        "--nss-dir",
        type=Path,
        default=Path("nss"),
        help="Directory to clone NSS into (default: ./nss)",
    )
    parser.add_argument(
        "--worktree-dir",
        type=Path,
        default=Path("nss-worktree"),
        help="Directory for the git worktree (default: ./nss-worktree)",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=Path("logs"),
        help="Directory for log files (default: ./logs)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Show debug output on the console (always written to log file)",
    )
    args = parser.parse_args()

    logger = setup_logging(args.log_dir, debug=args.debug)

    # Check required env vars
    bz_api_key = os.environ.get("BUGZILLA_API_KEY")
    if not bz_api_key:
        logger.error("BUGZILLA_API_KEY environment variable is not set")
        sys.exit(1)

    phab_api_token = os.environ.get("PHABRICATOR_API_TOKEN")
    if not phab_api_token:
        logger.error("PHABRICATOR_API_TOKEN environment variable is not set")
        sys.exit(1)

    # Read bug numbers
    bug_file: Path = args.bug_file
    if not bug_file.exists():
        logger.error("Bug file not found: %s", bug_file)
        sys.exit(1)

    bug_ids = [
        line.strip()
        for line in bug_file.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    ]
    logger.info("Loaded %d bug(s) from %s", len(bug_ids), bug_file)

    # Clone / update NSS
    nss_dir = args.nss_dir.resolve()
    clone_nss(nss_dir, logger)

    # Create a single worktree
    worktree_dir = args.worktree_dir.resolve()
    create_worktree(nss_dir, worktree_dir, logger)

    # For each bug: find revisions, resolve stacks, check statuses, apply
    succeeded: list[tuple[str, int]] = []
    failed: list[tuple[str, int, str]] = []
    skipped_bugs: list[str] = []

    for bug_id in bug_ids:
        logger.info("--- Processing bug %s ---", bug_id)

        # Step 1: Get revision IDs from Bugzilla
        rev_ids = get_revisions_for_bug(bug_id, bz_api_key, logger)
        if not rev_ids:
            skipped_bugs.append(bug_id)
            continue

        # Step 2: For each revision, find the stack tip and check statuses
        # Deduplicate tips — multiple revisions on one bug may share a stack
        tips_seen: set[int] = set()

        for rev_id in rev_ids:
            try:
                tip_id, stack_revs = phab_find_stack_tip(
                    rev_id, phab_api_token, logger
                )
            except RuntimeError as e:
                logger.error("  Bug %s: D%d — %s", bug_id, rev_id, e)
                failed.append((bug_id, rev_id, str(e)))
                continue

            # Check and warn about non-approved revisions in the stack
            check_revision_statuses(stack_revs, bug_id, logger)

            if tip_id in tips_seen:
                logger.info(
                    "  D%d shares stack tip D%d (already queued), skipping",
                    rev_id, tip_id,
                )
                continue
            tips_seen.add(tip_id)

            # Step 3: Apply the stack via moz-phab
            ok = apply_stack(tip_id, worktree_dir, logger)
            if ok:
                succeeded.append((bug_id, tip_id))
            else:
                failed.append((bug_id, tip_id, "moz-phab patch failed"))

    # Summary
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info(
        "  Stacks applied: %d | Failed: %d | Bugs with no revisions: %d",
        len(succeeded),
        len(failed),
        len(skipped_bugs),
    )
    if succeeded:
        logger.info("  Succeeded:")
        for bug_id, tip_id in succeeded:
            logger.info("    Bug %s: stack tip D%d", bug_id, tip_id)
    if failed:
        logger.info("  Failed:")
        for bug_id, rev_id, reason in failed:
            logger.info("    Bug %s: D%d — %s", bug_id, rev_id, reason)
    if skipped_bugs:
        logger.info("  Skipped (no revisions): %s", ", ".join(skipped_bugs))
    logger.info("  Worktree: %s", worktree_dir)
    logger.info("=" * 60)


def setup_logging(log_dir: Path, *, debug: bool = False) -> logging.Logger:
    logger = logging.getLogger("nss-patch-tool")
    logger.setLevel(logging.DEBUG)

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if debug else logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)

    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fh = logging.FileHandler(log_dir / f"apply_patches_{timestamp}.log")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger


if __name__ == "__main__":
    main()
