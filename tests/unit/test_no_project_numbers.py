"""No tracked text file names another repository by a project number.

Other repositories are named by their repository names. This guard reads every
tracked text file and fails on a project number: the word for a project,
singular or plural, followed by a space or a hyphen and a digit 1 to 5; a
capital P with such a digit as a whole word; or, in any case, the lower-case
word with a hyphen and such a digit. Identifiers such as p2_reader,
P2_READER_PASSWORD or project2_project3_sources.yaml are not project numbers and
do not match: the digit is followed by a word character, or no hyphen precedes it.

The patterns are written with character classes, so this file does not match
itself. The test is skipped outside a git checkout.
"""
from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
BINARY_PROBE_BYTES = 8192
MAX_REPORTED_HITS = 20

PROJECT_NUMBER_PATTERNS = (
    re.compile(r"\bProjects?[ -][1-5]\b"),
    re.compile(r"\bP[1-5]\b"),
    re.compile(r"project-[1-5]", re.IGNORECASE),
)


def tracked_files() -> list[Path]:
    """Return the tracked files of this checkout, or skip when there is none."""
    git = shutil.which("git")
    if git is None:
        pytest.skip("git is not installed")
    result = subprocess.run(
        [git, "-C", str(REPO_ROOT), "ls-files", "-z"],
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        pytest.skip("not a git checkout")
    names = result.stdout.decode("utf-8", errors="ignore").split("\0")
    return [REPO_ROOT / name for name in names if name]


def text_of(path: Path) -> str | None:
    """Return the file as text, or None for a binary or missing file."""
    try:
        data = path.read_bytes()
    except OSError:
        return None
    if b"\0" in data[:BINARY_PROBE_BYTES]:
        return None
    return data.decode("utf-8", errors="ignore")


def project_number_hits(paths: list[Path]) -> list[str]:
    """Return file:line for every line that holds a project number."""
    hits: list[str] = []
    for path in paths:
        text = text_of(path)
        if text is None:
            continue
        for number, line in enumerate(text.splitlines(), start=1):
            if matches(line):
                hits.append(f"{path.relative_to(REPO_ROOT)}:{number}")
    return hits


def matches(text: str) -> bool:
    """True when the text holds a project number."""
    return any(pattern.search(text) for pattern in PROJECT_NUMBER_PATTERNS)


def test_identifiers_are_not_project_numbers() -> None:
    identifiers = ("p2_reader", "P2_READER_PASSWORD", "project2", "project2_project3_sources.yaml")
    for identifier in identifiers:
        assert not matches(identifier), identifier


def test_the_patterns_catch_project_numbers() -> None:
    # Each label is split into two literals, so this file holds none of them.
    labels = ("Pro" "ject 2", "Pro" "jects 1", "Pro" "ject-4", "P" "5", "P" "2-03", "pro" "ject-2")
    for label in labels:
        assert matches(label), label


def test_no_tracked_text_file_names_a_project_number() -> None:
    hits = project_number_hits(tracked_files())
    shown = "\n".join(hits[:MAX_REPORTED_HITS])
    hidden = len(hits) - MAX_REPORTED_HITS
    more = f"\n... and {hidden} more" if hidden > 0 else ""
    assert not hits, f"project numbers in tracked files:\n{shown}{more}"
