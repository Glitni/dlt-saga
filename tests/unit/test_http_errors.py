"""Error-response bodies are summarised before they reach an error message."""

import pytest
import requests

from dlt_saga.utility.http_errors import response_body_snippet
from dlt_saga.utility.secrets import redaction

pytestmark = pytest.mark.unit


def _response(body: str, content_type: str = "") -> requests.Response:
    resp = requests.Response()
    resp.status_code = 500
    if content_type:
        resp.headers["Content-Type"] = content_type
    resp._content = body.encode()
    resp.encoding = "utf-8"
    return resp


HTML_PAGE = (
    '<html lang="en"><head><title>Example &amp; Co\n  Error</title>'
    "<style>body{margin:0}</style></head>"
    '<body><img src="data:image/png;base64,iVBORw0KGgo"></body></html>'
)


def test_html_reduced_to_title():
    resp = _response(HTML_PAGE, "text/html; charset=utf-8")
    assert response_body_snippet(resp) == "Example & Co Error"


def test_html_detected_without_content_type():
    resp = _response("<!DOCTYPE html>\n" + HTML_PAGE)
    assert response_body_snippet(resp) == "Example & Co Error"


def test_html_without_title_yields_nothing():
    resp = _response("<html><body>oops</body></html>", "text/html")
    assert response_body_snippet(resp) == ""


def test_json_body_kept_on_one_line():
    resp = _response('{\n  "error": "bad date"\n}', "application/json")
    assert response_body_snippet(resp) == '{ "error": "bad date" }'


def test_xml_body_is_not_treated_as_html():
    resp = _response("<error><message>No results</message></error>", "text/xml")
    assert response_body_snippet(resp) == (
        "<error><message>No results</message></error>"
    )


def test_long_body_capped():
    resp = _response("x" * 2000, "text/plain")
    snippet = response_body_snippet(resp, limit=100)
    assert len(snippet) == 100
    assert snippet.endswith("…")


def test_empty_body():
    assert response_body_snippet(_response("   ")) == ""


def test_secret_redacted_before_capping():
    secret = "s3cret-token-value"
    redaction._reset_for_testing()
    redaction.register_secret(secret)
    try:
        # The cap falls inside the secret: masking first leaves no fragment.
        resp = _response("x" * 90 + secret, "text/plain")
        snippet = response_body_snippet(resp, limit=100)
    finally:
        redaction._reset_for_testing()
    assert "s3cret" not in snippet
    assert redaction.REDACTION_MASK in snippet
