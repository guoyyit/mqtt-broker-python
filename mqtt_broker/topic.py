"""MQTT 主题名校验与通配符匹配。

匹配规则（MQTT 3.1.1 §4.7）：
- 主题按 '/' 切分为若干“层级”；
- '+' 恰好匹配一个层级（只能独占整层，如 ``sport/+/score``）；
- '#' 匹配零个或多个层级，且只能出现在过滤器末尾并独占整层（``sport/#``）；
- ``sport/#`` 同时匹配 ``sport``（零层）；
- '$' 开头的系统主题不被以 '+' 或 '#' 开头的过滤器匹配（§4.7.2），
  但可被精确过滤器订阅。
"""

from __future__ import annotations


class TopicError(ValueError):
    """主题名或主题过滤器非法。"""


def _check_common(topic: str) -> None:
    if topic == "":
        raise TopicError("topic must not be empty")
    if "\x00" in topic:
        raise TopicError("topic must not contain NUL character")
    if len(topic.encode("utf-8")) > 65535:
        raise TopicError("topic exceeds 65535 bytes")


def validate_publish_topic(topic: str) -> None:
    """发布使用的主题名：非空、不含通配符。"""
    _check_common(topic)
    if "#" in topic or "+" in topic:
        raise TopicError("publish topic name must not contain wildcards")


def validate_filter(topic_filter: str) -> None:
    """订阅使用的主题过滤器：通配符只能独占整层。"""
    _check_common(topic_filter)
    levels = topic_filter.split("/")
    for index, level in enumerate(levels):
        if "#" in level:
            if level != "#" or index != len(levels) - 1:
                raise TopicError("'#' must be the last level and occupy the whole level")
        if "+" in level and level != "+":
            raise TopicError("'+' must occupy a whole level")


def topic_matches(topic_filter: str, topic_name: str) -> bool:
    """过滤器是否匹配具体主题名。"""
    # '$' 系统主题不被首层级为通配符的过滤器匹配。
    if topic_name.startswith("$") and topic_filter[:1] in ("+", "#"):
        return False

    filter_levels = topic_filter.split("/")
    name_levels = topic_name.split("/")

    i = 0
    while i < len(filter_levels):
        flevel = filter_levels[i]
        if flevel == "#":
            # 必须为末层：匹配剩余零个或多个层级。
            return True
        if i >= len(name_levels):
            return False
        if flevel != "+" and flevel != name_levels[i]:
            return False
        i += 1
    # 过滤器无 '#'，层数必须完全相等。
    return i == len(name_levels)
