#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "click",
#   "requests",
# ]
# ///

import json
import sys
from typing import TypedDict

import click
import requests

# Duplicates of structs from the changelog generator script


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


SPACER = {"type": "context", "elements": [{"type": "plain_text", "text": " "}]}
DIVIDER = {"type": "divider"}


def build_change_list_elements(
    changes: list[ChangeEntry], repository: str
) -> list[dict]:
    """Build rich_text_list elements for a list of changes"""
    elements = []
    for change in changes:
        sha_short = change["sha"][:7]
        commit_url = f"https://github.com/{repository}/commit/{change['sha']}"
        parts: list[dict] = []

        if change.get("scopes"):
            scopes_str = ", ".join(change["scopes"])
            parts.append(
                {"type": "text", "text": f"{scopes_str}: ", "style": {"bold": True}}
            )

        parts.extend(
            [
                {"type": "text", "text": f"{change['subject']} ("},
                {
                    "type": "link",
                    "url": commit_url,
                    "text": sha_short,
                    "style": {"code": True},
                },
                {"type": "text", "text": ")"},
            ]
        )

        elements.append({"type": "rich_text_section", "elements": parts})

    return elements


def build_changes_block(
    category: str, changes: list[ChangeEntry], repository: str
) -> dict:
    """Build a rich_text block for a category of changes"""
    return {
        "type": "rich_text",
        "elements": [
            {
                "type": "rich_text_section",
                "elements": [
                    {"type": "text", "text": f"{category}:", "style": {"bold": True}},
                    {"type": "text", "text": "\n"},
                ],
            },
            {
                "type": "rich_text_list",
                "style": "bullet",
                "indent": 0,
                "border": 0,
                "elements": build_change_list_elements(changes, repository),
            },
        ],
    }


def build_slack_blocks(changelog: Changelog, title: str, repository: str) -> list[dict]:
    """Render the changelog as Slack Block Kit blocks"""
    triggered_by = changelog.get("triggered_by")
    has_changes = any(changelog["new_changes"].values())
    has_dropped = any(changelog["dropped_changes"].values())

    blocks: list[dict] = [
        DIVIDER,
        {
            "type": "header",
            "text": {"type": "plain_text", "text": title, "emoji": True},
        },
    ]

    if triggered_by:
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"_triggered by @{triggered_by}_"}
                ],
            }
        )

    blocks.extend([SPACER, DIVIDER])

    if not has_changes and not has_dropped:
        blocks.extend(
            [
                SPACER,
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": "_no changes in this release_"},
                },
                SPACER,
                DIVIDER,
            ]
        )
        return blocks

    if has_changes:
        blocks.append(
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":sparkles: What's new",
                    "emoji": True,
                },
            }
        )
        for category, changes in changelog["new_changes"].items():
            if changes:
                blocks.append(build_changes_block(category, changes, repository))
        blocks.extend([SPACER, DIVIDER])

    if has_dropped:
        blocks.append(
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": ":put_litter_in_its_place: Reverted changes",
                    "emoji": True,
                },
            }
        )
        for category, changes in changelog["dropped_changes"].items():
            if changes:
                blocks.append(build_changes_block(category, changes, repository))
        blocks.extend([SPACER, DIVIDER])

    return blocks


@click.command()
@click.option("--title", required=True, help="Title/header for the Slack message")
@click.option(
    "--repository",
    envvar="GITHUB_REPOSITORY",
    required=True,
    help="GitHub repository (owner/repo)",
)
@click.option("--webhook-url", required=True, help="Slack webhook URL")
@click.option(
    "--dry/--no-dry", default=False, help="Dry run: print payload instead of posting"
)
def main(title: str, repository: str, webhook_url: str, dry: bool) -> None:
    """
    Publish changelog to Slack
    Accepts changelog JSON object generated by the changelog action via stdin
    """
    changelog: Changelog = json.load(sys.stdin)
    payload = {"blocks": build_slack_blocks(changelog, title, repository)}

    if dry:
        click.echo(json.dumps(payload, indent=2))
        return

    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        click.echo(f"Failed to post to Slack: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
