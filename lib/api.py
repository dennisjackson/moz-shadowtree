"""Bugzilla and Phabricator API clients, HTTP session factory."""

import json
import logging
import re
import subprocess
from pathlib import Path

import requests
import requests_cache
from requests.adapters import HTTPAdapter
from unidiff import PatchSet
from urllib3.util.retry import Retry

BUGZILLA_REST_URL = "https://bugzilla.mozilla.org/rest/bug"
PHABRICATOR_API_URL = "https://phabricator.services.mozilla.com/api/"

# Revision statuses considered "approved"
APPROVED_STATUSES = {"accepted"}
# Revision statuses that should be skipped entirely
SKIP_STATUSES = {"abandoned"}

DEFAULT_CACHE_DB = ".cache/lookups"
DEFAULT_CACHE_TTL = 3600  # 1 hour

STATUS_EMOJI = {
    "accepted": "\u2705",
    "needs-review": "\u23f3",
    "changes-planned": "\u23f3",
    "needs-revision": "\u23f3",
    "abandoned": "\U0001f5d1\ufe0f",
}


# ---------------------------------------------------------------------------
# HTTP session factory
# ---------------------------------------------------------------------------

def _git_commit_short() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, check=True,
        ).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


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
    session.headers["User-Agent"] = f"ShadowTree/{_git_commit_short()}"

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
# Phabricator Conduit client
# ---------------------------------------------------------------------------

class PhabClient:
    def __init__(self, api_token: str, session: requests.Session, logger: logging.Logger):
        self.token = api_token
        self.session = session
        self.logger = logger

    def call(self, method: str, args: dict) -> dict:
        url = PHABRICATOR_API_URL + method
        data = {
            "params": json.dumps(
                {**args, "__conduit__": {"token": self.token}},
                separators=(",", ":"),
            ),
            "output": "json",
            "__conduit__": True,
        }
        resp = self.session.post(url, data=data)
        resp.raise_for_status()
        result = resp.json()
        if result.get("error_code"):
            raise RuntimeError(
                f"Phabricator error: {result.get('error_info', result['error_code'])}"
            )
        return result["result"]

    def get_revisions(self, rev_ids: list[int]) -> list[dict]:
        result = self.call("differential.revision.search", {
            "constraints": {"ids": rev_ids},
            "attachments": {"reviewers": True},
        })
        return result.get("data", [])

    def _walk_stack_edges(self, phid: str, edge_type: str) -> list[str]:
        """Walk a single-direction edge chain (parent or child) from *phid*."""
        result: list[str] = []
        current = phid
        while True:
            edges = self.call("edge.search", {
                "sourcePHIDs": [current],
                "types": [edge_type],
            })
            edge_data = edges.get("data", [])
            if not edge_data:
                break
            if len(edge_data) > 1:
                dest_phids = [e["destinationPHID"] for e in edge_data]
                self.logger.warning(
                    "\u26a0\ufe0f  Non-linear stack at %s (%d %s edges: %s), stopping walk",
                    current, len(edge_data), edge_type, ", ".join(dest_phids),
                )
                break
            next_phid = edge_data[0]["destinationPHID"]
            result.append(next_phid)
            current = next_phid
        return result

    def find_stack_tip(self, rev_id: int) -> tuple[int, list[dict]]:
        """Given a revision ID, walk to the top *and* bottom of its stack.

        Returns (tip_rev_id, all_revisions_in_stack) ordered bottom-to-top.
        """
        revs = self.get_revisions([rev_id])
        if not revs:
            raise RuntimeError(f"Revision D{rev_id} not found on Phabricator")

        base_rev = revs[0]
        parent_phids = self._walk_stack_edges(base_rev["phid"], "revision.parent")
        children_phids = self._walk_stack_edges(base_rev["phid"], "revision.child")

        related_phids = parent_phids + children_phids
        if related_phids:
            related_revs = self.call("differential.revision.search", {
                "constraints": {"phids": related_phids},
                "attachments": {"reviewers": True},
            }).get("data", [])
            related_map = {r["phid"]: r for r in related_revs}
        else:
            related_map = {}

        parent_revs = [related_map[p] for p in reversed(parent_phids) if p in related_map]
        child_revs = [related_map[p] for p in children_phids if p in related_map]
        all_revs = parent_revs + [base_rev] + child_revs

        tip = all_revs[-1]
        tip_id = tip["id"]
        self.logger.debug(
            "\U0001f517 D%d stack: %s \u2192 tip D%d",
            rev_id,
            " \u2192 ".join(f"D{r['id']}" for r in all_revs),
            tip_id,
        )
        return tip_id, all_revs

    def get_revision_status_emojis(self, revisions: list[dict]) -> dict[int, str]:
        result: dict[int, str] = {}
        for rev in revisions:
            status = rev["fields"]["status"]["value"]
            rev_id = rev["id"]
            result[rev_id] = STATUS_EMOJI.get(status, "\u2753")
            self.logger.debug("D%d [%s] \u2192 %s", rev_id, status, result[rev_id])
        return result

    def get_revision_paths(self, revisions: list[dict]) -> dict[int, set[str]]:
        """Fetch the file paths touched by each revision."""
        if not revisions:
            return {}

        rev_phids = [r["phid"] for r in revisions]
        phid_to_id = {r["phid"]: r["id"] for r in revisions}

        all_diffs: list[dict] = []
        after = None
        limit = len(rev_phids) * 5
        while True:
            query: dict = {
                "constraints": {"revisionPHIDs": rev_phids},
                "order": "newest",
                "limit": limit,
            }
            if after is not None:
                query["after"] = after
            diffs_result = self.call("differential.diff.search", query)
            all_diffs.extend(diffs_result.get("data", []))
            cursor = diffs_result.get("cursor", {})
            after = cursor.get("after")
            if not after:
                break

        rev_phid_to_diff_id: dict[str, int] = {}
        for diff in all_diffs:
            rev_phid = diff["fields"].get("revisionPHID")
            if rev_phid and rev_phid not in rev_phid_to_diff_id:
                rev_phid_to_diff_id[rev_phid] = diff["id"]

        result: dict[int, set[str]] = {}
        for rev_phid, diff_id in rev_phid_to_diff_id.items():
            rev_id = phid_to_id.get(rev_phid)
            if rev_id is None:
                continue
            try:
                raw = self.call("differential.getrawdiff", {"diffID": diff_id})
                paths = {f.path for f in PatchSet(raw)} if isinstance(raw, str) else set()
                result[rev_id] = paths
                self.logger.debug("\U0001f4c4 D%d: %d file(s)", rev_id, len(paths))
            except RuntimeError as e:
                self.logger.warning("\u26a0\ufe0f  Could not fetch diff for D%d: %s", rev_id, e)
                result[rev_id] = set()

        return result


# ---------------------------------------------------------------------------
# Bugzilla
# ---------------------------------------------------------------------------

def _extract_revision_id(text: str) -> int | None:
    m = re.search(r"\bD(\d+)\b", text)
    return int(m.group(1)) if m else None


def get_revisions_for_bug(
    bug_id: str, bz_api_key: str, session: requests.Session, logger: logging.Logger
) -> list[int]:
    """Query Bugzilla for Phabricator revision attachments on a bug."""
    logger.debug("\U0001f50d Bug %s attachments", bug_id)

    try:
        resp = session.get(f"{BUGZILLA_REST_URL}/{bug_id}/attachment", headers={
            "Accept": "application/json",
            "X-BUGZILLA-API-KEY": bz_api_key,
        })
        resp.raise_for_status()
        data = resp.json()
    except requests.RequestException as e:
        logger.error("\u274c Bug %s: %s", bug_id, e)
        return []

    rev_ids: list[int] = []
    for attachment in data.get("bugs", {}).get(str(bug_id), []):
        if attachment.get("content_type") != "text/x-phabricator-request":
            continue
        extracted = _extract_revision_id(attachment.get("file_name", ""))
        if extracted is None:
            extracted = _extract_revision_id(attachment.get("summary", ""))
        if extracted is not None:
            rev_ids.append(extracted)
        else:
            logger.warning(
                "\u26a0\ufe0f  Bug %s: can't extract rev ID from: %s",
                bug_id,
                attachment.get("file_name") or attachment.get("summary"),
            )

    rev_ids = list(dict.fromkeys(rev_ids))

    if rev_ids:
        logger.debug("\U0001f41b Bug %s \u2192 %s", bug_id, ", ".join(f"D{r}" for r in rev_ids))
    else:
        logger.debug("Bug %s: no revisions found", bug_id)

    return rev_ids
