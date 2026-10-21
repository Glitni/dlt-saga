"""Summarise HTTP error response bodies for error messages."""

import html
import re

import requests

from dlt_saga.utility.secrets.redaction import redact

# Long enough for a typical JSON or plain-text error payload.
MAX_BODY_SNIPPET_CHARS = 500

_TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
_WHITESPACE_RE = re.compile(r"\s+")


def response_body_snippet(
    response: requests.Response, limit: int = MAX_BODY_SNIPPET_CHARS
) -> str:
    """Return a short, single-line summary of an error response's body.

    An HTML error page (a proxy or server error page) is reduced to its
    ``<title>``: its markup, inline styles and embedded images carry nothing the
    status code doesn't. Any other body is whitespace-collapsed, redacted and
    capped at ``limit`` characters — redacted before capping, since a secret cut
    in half no longer matches the log redaction filter.

    Returns:
        The summary, or ``""`` when there is nothing worth showing.
    """
    body = (response.text or "").strip()
    if not body:
        return ""
    if _is_html(response, body):
        match = _TITLE_RE.search(body)
        body = html.unescape(match.group(1)) if match else ""
    body = redact(_WHITESPACE_RE.sub(" ", body).strip())
    if len(body) > limit:
        body = body[: limit - 1] + "…"
    return body


def _is_html(response: requests.Response, body: str) -> bool:
    content_type = (response.headers.get("Content-Type") or "").lower()
    if "html" in content_type:
        return True
    head = body[:15].lower()
    return head.startswith("<!doctype html") or head.startswith("<html")
