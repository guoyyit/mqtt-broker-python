"""MQTT topic name / filter validation and wildcard matching.

Matching rules (MQTT 3.1.1 section 4.7):
- '+' matches exactly one topic level.
- '#' must be the last level and matches any number of remaining levels
  (including zero), e.g. "a/#" matches "a" itself.
- A filter not starting with '#' or '$' never matches a topic starting
  with '$' (system topics), and vice versa handled naturally by levels.
"""

from __future__ import annotations


def validate_topic_name(topic: str) -> bool:
    """A publish topic name must be non-empty, <=65535 bytes, no wildcards."""
    if not topic or len(topic.encode("utf-8")) > 65535:
        return False
    return "+" not in topic and "#" not in topic and "\x00" not in topic


def validate_topic_filter(topic_filter: str) -> bool:
    """A filter may contain '+' as a whole level and '#' only as final level."""
    if not topic_filter or len(topic_filter.encode("utf-8")) > 65535:
        return False
    if "\x00" in topic_filter:
        return False
    levels = topic_filter.split("/")
    for i, level in enumerate(levels):
        if "#" in level:
            # '#' must occupy an entire level and be the last level
            if level != "#" or i != len(levels) - 1:
                return False
        if "+" in level and level != "+":
            return False
    return True


def topic_matches(topic_filter: str, topic: str) -> bool:
    """Deterministic wildcard match of a topic against a filter."""
    filter_levels = topic_filter.split("/")
    topic_levels = topic.split("/")

    # Wildcard filters must not match $-system topics (MQTT 3.1.1 - 4.7.2).
    if topic_levels[0].startswith("$") and filter_levels[0] in ("+", "#"):
        return False

    for i, flevel in enumerate(filter_levels):
        if flevel == "#":
            return True  # matches all remaining levels, including zero
        if i >= len(topic_levels):
            return False
        if flevel != "+" and flevel != topic_levels[i]:
            return False
    return len(topic_levels) == len(filter_levels)
