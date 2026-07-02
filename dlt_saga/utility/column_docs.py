"""Encoding of saga classification labels into destination descriptions.

Classification labels (e.g. ``pii``) are folded into a table/column description
as a delimited, sentinel-marked block so every destination carries them in a
single portable, parseable field — no reliance on destination-native tag
mechanisms (which differ, and in BigQuery's case don't exist for free-form
column tags without a Data Catalog taxonomy).

Block format, always the trailing block of the description, on the same line::

    Primary contact email for the account owner. [saga:classification=contact,pii]

The block is ``[saga:`` + ``;``-separated ``key=value`` pairs + ``]``. List
values are ``,``-joined. Keys and list members are rendered **sorted** so an
unchanged config never re-renders — this is what makes the compose step (and
the later reconcile) idempotent.

The block is kept on a single line (no newline separator): destinations write
descriptions into SQL string literals, and some (e.g. BigQuery's table-level
``OPTIONS(description=...)``) do not escape the value — a raw newline would
produce an unclosed string literal.
"""

import re
from typing import Dict, List, Optional, Union

# Matches the trailing saga block (with any leading whitespace) so it can be
# stripped to recover the human-authored part of a description.
_SAGA_BLOCK_RE = re.compile(r"\s*\[saga:[^\]]*\]\s*$")

TMeta = Dict[str, Union[str, List[str]]]


def strip_saga_block(description: Optional[str]) -> str:
    """Return the human-authored part of a description, minus any saga block."""
    if not description:
        return ""
    return _SAGA_BLOCK_RE.sub("", description).rstrip()


def render_saga_block(meta: Optional[TMeta]) -> str:
    """Render a metadata mapping into a ``[saga:...]`` block, or "" if empty.

    Keys and list members are sorted for deterministic, idempotent output.
    Empty values (empty string / empty list) are dropped.
    """
    if not meta:
        return ""

    parts: List[str] = []
    for key in sorted(meta):
        value = meta[key]
        if isinstance(value, (list, tuple)):
            rendered = ",".join(sorted(str(v) for v in value))
        else:
            rendered = str(value)
        if rendered:
            parts.append(f"{key}={rendered}")

    return f"[saga:{';'.join(parts)}]" if parts else ""


def compose_description(human: Optional[str], meta: Optional[TMeta]) -> str:
    """Combine a human description with an encoded saga block.

    Any pre-existing saga block on ``human`` is stripped first, so composing is
    idempotent and safe to run against a value that already carries a block.
    """
    base = strip_saga_block(human)
    block = render_saga_block(meta)
    if not block:
        return base
    return f"{base} {block}" if base else block
