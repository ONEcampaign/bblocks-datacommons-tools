from typing import Annotated

from pydantic import BaseModel, PlainSerializer


def _ensure_quoted(s: str) -> str:
    if s.startswith("'") or s.startswith('"'):
        s = s.strip('"').strip("'")
    return f'"{s}"'


def mcf_quoted_str(value: str | list[str] | None) -> str | None:
    """Serialise str | list[str] -> quoted MCF string.

    * None  ➜ None
    * ["a", "b"] ➜ '"a", "b"'
    * 'foo'      ➜ '"foo"'
    * '"foo"'    ➜ '"foo"' (already quoted, left untouched)
    """
    """Serialize `str | list[str]` to an MCF-compatible quoted string.

        Rules
        -----
        • None           → None  
        • list[str]      → '"a", "b"'  (items are quoted only once)  
        • bare str       → '"foo"'  
        • already-quoted → left untouched
        """
    if value is None:
        return None

    if isinstance(value, list):
        if len(value) < 2:
            return _ensure_quoted(value[0])

        return ", ".join(_ensure_quoted(str(item)) for item in value)

    return _ensure_quoted(value)


QuotedStr = Annotated[
    str,
    PlainSerializer(
        _ensure_quoted,
        return_type=str | None,
        when_used="always",
    ),
]
QuotedStrList = Annotated[
    list[str],
    PlainSerializer(
        mcf_quoted_str,
        return_type=str | None,
        when_used="always",
    ),
]
