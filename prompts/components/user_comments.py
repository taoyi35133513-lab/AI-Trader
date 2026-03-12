"""User trade comments prompt component."""

USER_COMMENTS_TEMPLATE = """## 用户对近期交易的点评
用户对近期的交易操作提供了以下反馈，请在做交易决策时参考这些意见：

{comments}"""


def build_user_comments_section(comments: list[dict]) -> str:
    """Build the user comments section for the system prompt.

    Args:
        comments: List of comment dicts from CommentService.get_latest_comments()

    Returns:
        Formatted string, or empty string if no comments.
    """
    if not comments:
        return ""

    lines = []
    for c in comments:
        lines.append(
            f"- [{c['trade_date']}] {c['action'].upper()} {c['ts_code']}: {c['comment_text']}"
        )

    return USER_COMMENTS_TEMPLATE.format(comments="\n".join(lines))
