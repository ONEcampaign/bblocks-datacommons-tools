from bblocks.datacommons_tools.custom_data.models.common import (
    _ensure_quoted,
    mcf_quoted_str,
)


def test_ensure_quoted_handles_quotes_and_whitespace():
    """
    Ensures that strings already quoted or with whitespace
    are normalized to double-quoted form without extra spaces.
    """
    assert _ensure_quoted("value") == '"value"'
    assert _ensure_quoted("'value'") == '"value"'

def test_mcf_quoted_str_with_single_and_multiple_items():
    """
    Serializes strings and lists into MCF-compatible quoted strings.
    """
    # Single string
    assert mcf_quoted_str("abc") == '"abc"'
    # Single-element list
    assert mcf_quoted_str(["x"]) == '"x"'
    # Multi-element list
    multi = mcf_quoted_str(["a", "b", "c"])
    assert multi == '"a", "b", "c"'
    # None input
    assert mcf_quoted_str(None) is None
