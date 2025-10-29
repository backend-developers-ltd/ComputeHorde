import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

from matplotlib import colormaps, patches
import matplotlib.pyplot as plt

COLORMAP = colormaps.get_cmap("tab20c")

STATUS_COLORS = {
    "sent": COLORMAP(3),
    "received": COLORMAP(2),
    "accepted": COLORMAP(11),
    "streaming_ready": COLORMAP(10),
    "executor_ready": COLORMAP(10),
    "volumes_ready": COLORMAP(9),
    "execution_done": COLORMAP(8),
    "completed": COLORMAP(19),
    "rejected": COLORMAP(5),
    "failed": COLORMAP(4),
    "horde_failed": COLORMAP(4),
}


def _parse_timestamp(timestamp: str) -> datetime:
    if timestamp.endswith("Z"):
        timestamp = timestamp[:-1] + "+00:00"
    return datetime.fromisoformat(timestamp)


def _iter_job_entries(data: str) -> Iterable[tuple[str, list[tuple[datetime, str]]]]:
    for line in data.splitlines():
        line = line.strip()
        if not line:
            continue

        entry = json.loads(line)
        if entry.get("kind") != "job":
            continue

        payload = entry.get("payload") or {}
        status_history = payload.get("status_history") or []
        if not status_history:
            continue

        job_uuid = payload.get("uuid") or entry.get("id")
        if not job_uuid:
            continue

        events = []
        for status_entry in status_history:
            created_at = status_entry.get("created_at")
            status = status_entry.get("status")
            if not created_at or not status:
                continue
            events.append((_parse_timestamp(created_at), status))

        if not events:
            continue

        events.sort(key=lambda item: item[0])
        yield job_uuid, events


def plot(filename: str, data: str) -> None:
    print(f"Loaded {filename}")
    job_timelines = {job_uuid: events for job_uuid, events in _iter_job_entries(data)}

    if not job_timelines:
        print("No job entries with status history found. Skipping plot generation.")
        return

    earliest_time = min(events[0][0] for events in job_timelines.values())
    latest_time = max(events[-1][0] for events in job_timelines.values())

    if latest_time <= earliest_time:
        latest_time = earliest_time + timedelta(seconds=1)

    total_seconds = (latest_time - earliest_time).total_seconds()
    padding_seconds = max(total_seconds * 0.05, 1.0)
    final_padding_seconds = padding_seconds
    minimal_width_seconds = 0.5
    x_min = 0.0
    x_max = total_seconds + final_padding_seconds

    def executor_ready_key(item: tuple[str, list[tuple[datetime, str]]]) -> datetime:
        _, events = item
        for event_time, status in events:
            if status == "executor_ready":
                return event_time
        return events[-1][0]

    sorted_jobs = sorted(job_timelines.items(), key=executor_ready_key)
    total_jobs = len(sorted_jobs)

    row_spacing = 0.75
    fig_height = max(2.5, len(sorted_jobs) * row_spacing * 0.8 + 1.0)
    fig, ax = plt.subplots(figsize=(12, fig_height))

    used_statuses: set[str] = set()

    for job_index, (job_uuid, events) in enumerate(sorted_jobs):
        display_index = (total_jobs - job_index - 1) * row_spacing
        for event_index, (start_dt, status) in enumerate(events):
            start_num = (start_dt - earliest_time).total_seconds()
            if event_index + 1 < len(events):
                end_num = (events[event_index + 1][0] - earliest_time).total_seconds()
            else:
                end_num = start_num + final_padding_seconds

            if end_num <= start_num:
                end_num = start_num + minimal_width_seconds

            width = end_num - start_num

            color = STATUS_COLORS.get(status, "#cccccc")
            used_statuses.add(status)

            ax.barh(
                display_index,
                width,
                left=start_num,
                height=0.6,
                color=color,
            )

    ax.set_yticks(
        [(total_jobs - index - 1) * row_spacing for index in range(total_jobs)]
    )
    ax.set_yticklabels([str(index + 1) for index in range(total_jobs)])
    ax.set_ylabel("jobs")
    ax.set_xlabel("seconds")
    ax.set_xlim(x_min, x_max)

    legend_statuses = [status for status in STATUS_COLORS if status in used_statuses]
    if legend_statuses:
        legend_handles = [
            patches.Patch(
                color=STATUS_COLORS[status],
                label=status.replace("_", " ").title(),
            )
            for status in legend_statuses
        ]
        ax.legend(
            handles=legend_handles,
            loc="upper right",
            bbox_to_anchor=(0.98, 0.98),
            frameon=True,
            framealpha=0.9,
            facecolor="white",
            edgecolor="#dddddd",
            ncol=1,
        )

    half_span = row_spacing / 2
    ax.set_ylim(-half_span, (total_jobs - 1) * row_spacing + half_span)
    ax.grid(axis="x", linestyle="--", alpha=0.3)

    fig.tight_layout()

    output_path = Path(filename).with_suffix(".png")
    fig.savefig(output_path, dpi=150)
    plt.close(fig)

    print(f"Saved {output_path}")


def main():
    for filename in sorted(os.listdir(".")):
        if filename.endswith(".jsonl"):
            with open(filename, "rb") as f:
                data = f.read()
                plot(filename, data.decode("utf8"))


if __name__ == "__main__":
    main()
