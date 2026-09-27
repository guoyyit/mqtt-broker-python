"""主题名校验与单/多层通配符匹配语义。"""

from __future__ import annotations

import pytest

from mqtt_broker.topic import (TopicError, topic_matches, validate_filter,
                               validate_publish_topic)


@pytest.mark.parametrize(
    "topic_filter,topic_name,expected",
    [
        # 精确匹配
        ("sport/tennis/player1", "sport/tennis/player1", True),
        ("sport/tennis/player1", "sport/tennis/player2", False),
        # 单层通配符 '+'
        ("sport/+/player1", "sport/tennis/player1", True),
        ("sport/+/player1", "sport/football/player1", True),
        ("sport/+/player1", "sport/tennis/player2", False),
        ("+", "sport", True),
        ("+", "sport/tennis", False),
        ("a/+/c", "a/b/c", True),
        ("a/+/c", "a/b/x/c", False),
        ("/+", "/sport", True),
        ("+/", "sport/", True),
        # 多层通配符 '#'
        ("sport/tennis/player1/#", "sport/tennis/player1", True),
        ("sport/tennis/player1/#", "sport/tennis/player1/ranking", True),
        ("sport/tennis/player1/#", "sport/tennis/player1/score/wimbledon", True),
        ("sport/#", "sport", True),           # '#' 匹配零层
        ("sport/#", "sport/", True),
        ("sport/#", "sport/x/y/z", True),
        ("#", "sport/tennis/player1", True),
        ("#", "sport", True),
        ("#", "$SYS/uptime", False),          # '$' 系统主题例外
        ("+/monitor", "$SYS/monitor", False),
        ("$SYS/monitor", "$SYS/monitor", True),
        # 非匹配
        ("a/b", "a/b/", False),
        ("a/b/", "a/b", False),
        ("a/+/c", "a/c", False),
        ("a/+", "a/b/c", False),
    ],
)
def test_topic_matches(topic_filter, topic_name, expected):
    assert topic_matches(topic_filter, topic_name) is expected


@pytest.mark.parametrize(
    "invalid_filter",
    ["", "a/#/b", "a#", "a+b", "a/b#", "#/a", "sport/tennis#", "sport/+/tennis#"],
)
def test_invalid_filters(invalid_filter):
    with pytest.raises(TopicError):
        validate_filter(invalid_filter)


@pytest.mark.parametrize("valid_filter", ["a/b", "a/+/b", "a/#", "+", "#", "a/"])
def test_valid_filters(valid_filter):
    validate_filter(valid_filter)


@pytest.mark.parametrize("invalid_topic", ["", "a/+", "a/#", "a/+/b"])
def test_invalid_publish_topics(invalid_topic):
    with pytest.raises(TopicError):
        validate_publish_topic(invalid_topic)
