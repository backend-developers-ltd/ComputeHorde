#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "click",
#   "gitpython",
# ]
# ///

import json
import re
import subprocess
from collections.abc import Iterable
from typing import TypedDict

import click
from git import Repo
from git.objects.commit import Commit


class ChangeEntry(TypedDict):
    subject: str
    sha: str
    scopes: list[str]
    patch_id: str
    impacted_projects: list[str]
    type: str


class Changelog(TypedDict):
    new_ref: str
    previous_ref: str | None
    triggered_by: str | None
    new_changes: dict[str, list[ChangeEntry]]
    dropped_changes: dict[str, list[ChangeEntry]]
    deduplicated_groups: list[list[ChangeEntry]]


def parse_conventional_commit(subject: str) -> tuple[str, list[str], str]:
    """
    Parse conventional commit type, scopes, and cleaned subject from subject line
    If the subject doesn't look like a conventional commit, returns the subject as-is with type "_unknown"
    Supports multiple comma-separated scopes
    """
    # Supported formats:
    # type(scope1,scope2): subject
    # type: subject
    match = re.match(r"^([a-z]+)(?:\(([^)]+)\))?:\s*(.+)", subject)
    if match:
        commit_type = match.group(1)
        scope_str = match.group(2)
        cleaned_subject = match.group(3)
        scopes = [s.strip() for s in scope_str.split(",")] if scope_str else []
        return commit_type, scopes, cleaned_subject
    return "_unknown", [], subject


def calculate_patch_id(repo: Repo, commit: Commit) -> str:
    """
    Calculate stable patch-id for a commit
    Returns the commit SHA for merge commits or commits with no diff
    """
    result = subprocess.run(
        f"git show {commit.hexsha} | git patch-id --stable",
        shell=True,
        capture_output=True,
        text=True,
        check=True,
        cwd=repo.working_dir,
    )
    return result.stdout.split()[0] if result.stdout.strip() else commit.hexsha


def extract_impacted_projects(commit: Commit) -> list[str]:
    """Extract impacted projects from commit trailers"""
    impacted_projects = []
    for key, value in commit.trailers_list:
        if key.lower() == "impacts":
            impacted_projects.extend(v.strip() for v in value.split(","))
    return impacted_projects


def parse_commit(repo: Repo, commit: Commit) -> ChangeEntry:
    """Parse a gitpython commit"""
    category, scopes, subject = parse_conventional_commit(commit.summary)
    patch_id = calculate_patch_id(repo, commit)
    impacted_projects = extract_impacted_projects(commit)

    return {
        "subject": subject,
        "sha": commit.hexsha,
        "scopes": scopes,
        "patch_id": patch_id,
        "impacted_projects": impacted_projects,
        "type": category,
    }


def deduplicate(
    new: list[ChangeEntry], dropped: list[ChangeEntry]
) -> tuple[list[ChangeEntry], list[ChangeEntry], list[list[ChangeEntry]]]:
    """
    Exclude entries that exist in both lists by patch-id
    This prevents cherry-picked commits from appearing as both new and dropped changes when force-pushing
    Returns: (filtered_new, filtered_dropped, deduplicated_groups)
    where deduplicated_groups contains lists of duplicate entries with identical patch-ids
    """
    if not new or not dropped:
        # Nothing to compare. Don't bother calculating anything.
        return new, dropped, []

    new_patch_ids = {change["patch_id"] for change in new}
    dropped_patch_ids = {change["patch_id"] for change in dropped}
    overlapping = new_patch_ids & dropped_patch_ids

    deduplicated_groups = []
    for overlap in overlapping:
        deduplicated_groups.append(
            [
                *(change for change in new if change["patch_id"] == overlap),
                *(change for change in dropped if change["patch_id"] == overlap),
            ]
        )

    filtered_new = [e for e in new if e["patch_id"] not in overlapping]
    filtered_dropped = [e for e in dropped if e["patch_id"] not in overlapping]

    return filtered_new, filtered_dropped, deduplicated_groups


def filter_changes(
    changes: list[ChangeEntry],
    project: str | None,
    exclude_commit_types: Iterable[str],
) -> list[ChangeEntry]:
    """
    Filter entries by project and conventional commit type
    If project is None, all projects and non-project commits are included
    """
    exclude_commit_types = set(exclude_commit_types)

    if project is not None:
        # Limit to the specific project
        changes = (
            change for change in changes if project in change["impacted_projects"]
        )

    # Filter out unwanted commit types
    changes = (
        change for change in changes if change["type"] not in set(exclude_commit_types)
    )

    return list(changes)


@click.command()
@click.option(
    "--new-ref", default="HEAD", help="New ref (commit, branch, tag) to compare"
)
@click.option(
    "--previous-ref",
    default=None,
    help="Previous ref to compare (defaults to first commit in repo)",
)
@click.option("--project", help="Include only changes impacting this project")
@click.option("--exclude", help="Comma-separated list of commit types to exclude")
@click.option("--triggered-by", help="GitHub username who triggered the release")
def main(
    new_ref: str,
    previous_ref: str | None,
    project: str | None,
    exclude: str | None,
    triggered_by: str | None,
) -> None:
    """
    Generate a changelog between given refs
    Outputs a JSON object containing new and dropped changes, grouped by conventional commit type.
    Includes parsed conventional commit metadata and impacted projects for each change.

    Example output:
    {
      "new_ref": "v1.2.0",
      "previous_ref": "v1.1.0",
      "new_changes": {
        "feat": [{"subject": "add new feature", "sha": "abc123...", "scopes": ["api"], ...}],
        "fix": [{"subject": "fix bug", "sha": "def456...", "scopes": [], ...}]
      },
      "dropped_changes": {},
      "deduplicated_groups": []
    }
    """
    repo = Repo(".")
    exclude_types = [t.strip() for t in exclude.split(",")] if exclude else []

    if previous_ref is None:
        # First release: consider all commits in the history of given ref
        new_commits = list(repo.iter_commits(new_ref))
        dropped_commits = []
    else:
        # Subsequent release: consider commits between given refs, including "new" ref and excluding "previous" ref
        new_commits = list(repo.iter_commits(f"{previous_ref}..{new_ref}"))
        dropped_commits = list(repo.iter_commits(f"{new_ref}..{previous_ref}"))

    # Parse all commits into "changes"
    new_changes = [parse_commit(repo, c) for c in new_commits]
    dropped_changes = [parse_commit(repo, c) for c in dropped_commits]

    # Detect commits that would be reported as both new and dropped (e.g., force pushing cherry-picks)
    new_changes, dropped_changes, deduplicated_groups = deduplicate(
        new_changes, dropped_changes
    )

    # Filter by project and conventional commit type
    new_changes = filter_changes(new_changes, project, exclude_types)
    dropped_changes = filter_changes(dropped_changes, project, exclude_types)

    # Categorize by conventional commit type
    categorized_new = {}
    for entry in new_changes:
        categorized_new.setdefault(entry["type"], []).append(entry)

    categorized_dropped = {}
    for entry in dropped_changes:
        categorized_dropped.setdefault(entry["type"], []).append(entry)

    changelog: Changelog = {
        "new_ref": new_ref,
        "previous_ref": previous_ref,
        "triggered_by": triggered_by,
        "new_changes": categorized_new,
        "dropped_changes": categorized_dropped,
        "deduplicated_groups": deduplicated_groups,
    }

    json.dump(changelog, fp=click.get_text_stream("stdout"), indent=2)


if __name__ == "__main__":
    main()
