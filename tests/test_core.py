"""Unit tests for the shared core layer's error branches.

The CLI and MCP tests cover the happy paths; these target the small set of
validation branches that the typed layers above ``core`` cannot reach.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from laneq import core


def test_invalid_priority_is_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LANEQ_DB", str(tmp_path / "queue.db"))
    with pytest.raises(core.QueueError, match="invalid priority"):
        core.push("body", priority="P9")


def test_touch_missing_taken_item_raises(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LANEQ_DB", str(tmp_path / "queue.db"))
    core.push("not taken", priority="P0")
    with pytest.raises(core.QueueError, match="no taken item #1"):
        core.touch(1)
