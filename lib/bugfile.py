"""Bug list file parser."""

import dataclasses
from pathlib import Path

DEFAULT_BRANCH = "main"


@dataclasses.dataclass
class BugFileConfig:
    repo_url: str
    branch: str
    tag: str | None
    name: str
    bug_ids: list[str]


def parse_bug_file(bug_file: Path) -> BugFileConfig:
    directives: dict[str, str] = {}
    bug_ids: list[str] = []

    for line in bug_file.read_text().splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("#"):
            comment = stripped.lstrip("#").strip()
            key, _, value = comment.partition(":")
            if key.lower() in {"repo", "branch", "tag", "name"} and value:
                directives[key.lower()] = value.strip()
        else:
            bug_ids.append(stripped)

    if "tag" in directives and "branch" in directives:
        raise ValueError("Bug file specifies both 'branch' and 'tag' \u2014 use one or the other")
    if "repo" not in directives:
        raise ValueError("Bug file must include a '# repo: <url>' directive")

    return BugFileConfig(
        repo_url=directives["repo"],
        branch=directives.get("branch", DEFAULT_BRANCH),
        tag=directives.get("tag"),
        name=directives.get("name", bug_file.stem),
        bug_ids=bug_ids,
    )
