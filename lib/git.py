"""Git operations: clone, worktree management, patch application, conflict diagnosis."""

import logging
import shutil
import subprocess
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# Git helpers
# ---------------------------------------------------------------------------

def _run_git(args: list[str], logger: logging.Logger, **kwargs) -> subprocess.CompletedProcess:
    check = kwargs.pop("check", False)
    result = subprocess.run(args, capture_output=True, text=True, **kwargs)
    if result.stdout.strip():
        logger.debug("%s", result.stdout.strip())
    if result.stderr.strip():
        logger.debug("%s", result.stderr.strip())
    if check and result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, args)
    return result


def _git_lines(args: list[str], logger: logging.Logger, **kwargs) -> set[str]:
    r = _run_git(args, logger, **kwargs)
    return set(r.stdout.strip().splitlines()) if r.returncode == 0 and r.stdout.strip() else set()


def clone_repo(dest: Path, repo_url: str, branch: str, logger: logging.Logger,
               *, tag: str | None = None) -> None:
    if (dest / ".git").exists():
        logger.debug("\U0001f4e6 Updating repo at %s", dest)
        _run_git(["git", "fetch", "origin"], logger, cwd=dest, check=True)
        ref = tag or branch
        _run_git(["git", "checkout", ref], logger, cwd=dest, check=True)
        if not tag:
            _run_git(["git", "reset", "--hard", f"origin/{branch}"], logger, cwd=dest, check=True)
    else:
        logger.debug("\U0001f4e6 Cloning %s \u2192 %s", repo_url, dest)
        ref = tag or branch
        _run_git(["git", "clone", repo_url, str(dest)], logger, check=True)
        _run_git(["git", "checkout", ref], logger, cwd=dest, check=True)
        if not tag:
            _run_git(["git", "reset", "--hard", f"origin/{branch}"], logger, cwd=dest, check=True)


def _cleanup_old_branches(repo: Path, logger: logging.Logger) -> None:
    for branch in _git_lines(["git", "branch", "--list", "patch-apply-*"], logger, cwd=repo):
        branch = branch.strip().lstrip("* ")
        logger.debug("\U0001f9f9 Deleting old branch %s", branch)
        _run_git(["git", "branch", "-D", branch], logger, cwd=repo)


def create_worktree(repo: Path, worktree: Path, branch: str, logger: logging.Logger,
                    *, tag: str | None = None) -> None:
    branch_name = f"patch-apply-{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}"
    start_point = tag if tag else f"origin/{branch}"
    if worktree.exists():
        logger.debug("\U0001f33f Removing old worktree %s", worktree)
        result = _run_git(
            ["git", "worktree", "remove", "--force", str(worktree)],
            logger, cwd=repo,
        )
        if result.returncode != 0 and worktree.exists():
            logger.debug("\U0001f33f git worktree remove failed; falling back to rmtree")
            shutil.rmtree(worktree)
    _cleanup_old_branches(repo, logger)
    logger.debug("\U0001f33f Worktree %s [%s] from %s", worktree, branch_name, start_point)
    _run_git(
        ["git", "worktree", "add", "-b", branch_name, str(worktree), start_point],
        logger, cwd=repo, check=True,
    )


# ---------------------------------------------------------------------------
# Patch application
# ---------------------------------------------------------------------------

def verify_worktree_clean(worktree: Path, logger: logging.Logger) -> bool:
    result = _run_git(["git", "status", "--porcelain"], logger, cwd=worktree)
    if result.returncode != 0:
        logger.error("\u274c git status failed: %s", result.stderr)
        return False
    if result.stdout.strip():
        logger.error("\u274c Worktree dirty, aborting:\n%s", result.stdout.strip())
        return False
    return True


def diagnose_conflict(
    conflict_files: set[str],
    stack_file_map: dict[int, set[str]],
    applied_files: dict[str, list[tuple[str, int]]],
    logger: logging.Logger,
) -> None:
    all_stack_files: set[str] = set()
    for paths in stack_file_map.values():
        all_stack_files |= paths

    if conflict_files:
        logger.error("\U0001f4a5 Conflicting files: %s", ", ".join(sorted(conflict_files)))

    candidate_files: set[str] = set()
    prior_hits: dict[tuple[str, int], list[str]] = {}
    for f in all_stack_files:
        for key in applied_files.get(f, []):
            candidate_files.add(f)
            prior_hits.setdefault(key, []).append(f)

    if candidate_files:
        logger.error(
            "\U0001f500 Candidate clashes with prior patches: %s",
            ", ".join(sorted(candidate_files)),
        )
        for rev_id in sorted(stack_file_map):
            overlap = sorted(candidate_files & stack_file_map[rev_id])
            if overlap:
                logger.error("   D%d: %s", rev_id, ", ".join(overlap))
        for (bug_id, tip_id), files in sorted(prior_hits.items()):
            logger.error(
                "   \u21b3 Bug %s (D%d): %s", bug_id, tip_id, ", ".join(sorted(files))
            )
    else:
        logger.error("\U0001f332 No overlap with prior patches \u2014 likely conflicts with base tree")


def apply_stack(
    tip_rev_id: int,
    worktree: Path,
    logger: logging.Logger,
    *,
    stack_file_map: dict[int, set[str]] | None = None,
    applied_files: dict[str, list[tuple[str, int]]] | None = None,
) -> tuple[bool, set[str]]:
    """Apply the full stack ending at tip_rev_id using moz-phab patch."""
    revision_str = f"D{tip_rev_id}"

    if not verify_worktree_clean(worktree, logger):
        return False, set()

    head_result = _run_git(["git", "rev-parse", "HEAD"], logger, cwd=worktree)
    if head_result.returncode != 0:
        logger.error("\u274c Can't determine HEAD in %s", worktree)
        return False, set()
    pre_patch_head = head_result.stdout.strip()

    logger.debug("\U0001f4ce Applying %s", revision_str)

    result = subprocess.run(
        ["moz-phab", "patch", "--apply-to", "here", "--yes", revision_str],
        cwd=worktree, capture_output=True, text=True,
    )

    if result.returncode != 0:
        logger.error("\u274c %s failed (exit %d)", revision_str, result.returncode)
        logger.debug("stdout:\n%s", result.stdout)
        logger.debug("stderr:\n%s", result.stderr)

        if stack_file_map is not None:
            conflict_files = _git_lines(
                ["git", "diff", "--name-only"], logger, cwd=worktree,
            )
            diagnose_conflict(
                conflict_files, stack_file_map, applied_files or {}, logger,
            )

        _run_git(["git", "reset", "--hard", pre_patch_head], logger, cwd=worktree)
        _run_git(["git", "clean", "-fd"], logger, cwd=worktree)
        return False, set()

    modified_files = _git_lines(
        ["git", "diff", "--name-only", f"{pre_patch_head}..HEAD"],
        logger, cwd=worktree,
    )

    logger.debug("moz-phab stdout:\n%s", result.stdout)
    if result.stderr:
        logger.debug("moz-phab stderr:\n%s", result.stderr)
    logger.debug("\u2705 %s applied (%d files)", revision_str, len(modified_files))
    return True, modified_files
