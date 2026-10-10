"""Offline transport contracts; all people, groups and messages are synthetic."""

import importlib.util
import io
import json
import urllib.error
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import pytest

SCRIPT = (
    Path(__file__).resolve().parents[2]
    / "skills/public/telegram-research/scripts/vox.py"
)
spec = importlib.util.spec_from_file_location("telegram_research_vox", SCRIPT)
vox = importlib.util.module_from_spec(spec)
spec.loader.exec_module(vox)


@pytest.fixture
def transport(monkeypatch):
    monkeypatch.setenv("VOX_API_KEY", "synthetic-test-credential")
    monkeypatch.setattr("time.sleep", lambda _: None)
    requests = []
    responses = []

    def open_request(req, timeout):
        requests.append(req)
        result = responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return io.BytesIO(
            result if isinstance(result, bytes) else json.dumps(result).encode("utf-8")
        )

    monkeypatch.setattr(vox.OPENER, "open", open_request)
    return requests, responses


def run_cli(capsys, *args):
    invoke(list(args))
    return json.loads(capsys.readouterr().out)


def invoke(args):
    with patch("sys.argv", ["vox.py", *args]):
        vox.main()


def public_card(gid=-10012345):
    return {
        "id": gid,
        "alias": "example_chat",
        "type": "CHAT",
        "is_private": False,
        "related_group": 0,
    }


def test_search_preserves_evidence_author_links_and_partial_cursor(transport, capsys):
    requests, responses = transport
    text = "а" * 900 + " https://t.me/next_community"
    message = {
        "id": 17,
        "group_id": -10012345,
        "user_id": 98765,
        "user_alias": "example",
        "text": text,
        "date": "2026-09-01T12:00:00Z",
        "message_url": "https://t.me/example_chat/17",
        "related_group_id": -10012346,
        "related_group_url": "https://t.me/example_channel",
    }
    responses.append(
        {
            "messages": [message],
            "count": 3,
            "stop_reason": "timeout",
            "stopped_at": message["date"],
        }
    )
    out = run_cli(
        capsys,
        "search",
        "подработка",
        "--limit",
        "20",
        "--from",
        "2026-09-01",
        "--to",
        "2026-10-01",
        "--type",
        "comment",
    )
    assert out["messages"][0] == message
    assert out["count"] == 3  # upstream count must not silently become returned length
    assert out["returned_count"] == 1
    assert out["stop_reason"] == "timeout"
    assert out["stopped_at"] == message["date"]
    query = parse_qs(urlsplit(requests[0].full_url).query)
    assert query["query"] == ["подработка"]
    assert query["from_date"] == ["2026-09-01"]
    assert query["message_type"] == ["comment"]
    assert (
        "offset" not in query and "language" not in query and "search_type" not in query
    )
    assert requests[0].get_header("Authorization") == "Bearer synthetic-test-credential"


def test_explicit_text_preview_marks_truncation(transport, capsys):
    _, responses = transport
    responses.append({"messages": [{"text": "abcdef", "user_id": 7}], "count": 1})
    out = run_cli(capsys, "search", "abc", "--max-chars", "3")
    assert out["messages"][0]["text_truncated"] is True
    assert out["messages"][0]["text_length"] == 6
    assert out["messages"][0]["text"].startswith("abc")


def test_search_consumes_progress_until_final_result_and_then_stops(transport, capsys):
    requests, responses = transport
    final = {
        "messages": [{"id": 7, "user_id": 98765, "text": "public discussion"}],
        "count": 1,
        "stop_reason": "timeout",
        "stopped_at": "2026-08-10T00:00:00Z",
    }
    responses.append(
        b'{"point":"2026-10-09T00:00:00Z"}\n\n'
        + json.dumps(final).encode("utf-8")
        + b"\nnot part of the completed response"
    )
    out = run_cli(capsys, "search", "work", "--timeout", "600")
    params = parse_qs(urlsplit(requests[0].full_url).query)
    assert params["stream"] == ["true"]
    assert params["timeout"] == ["600"]
    assert out["messages"] == final["messages"]
    assert out["stop_reason"] == "timeout"
    assert out["stopped_at"] == final["stopped_at"]


@pytest.mark.parametrize(
    "body", [b"", b'{"point":0}\n', b'{"error":"provider failed"}\n']
)
def test_search_stream_without_final_result_is_not_an_empty_success(transport, body):
    requests, responses = transport
    responses.extend([body, body])
    with pytest.raises(SystemExit):
        invoke(["search", "work"])
    assert len(requests) == (1 if b'"error"' in body else 2)


def test_search_retries_a_stream_closed_after_progress(transport, capsys):
    requests, responses = transport
    responses.extend([b'{"point":0}\n', {"messages": [{"id": 7}], "count": 1}])
    out = run_cli(capsys, "search", "work")
    assert out["messages"][0]["id"] == 7
    assert len(requests) == 2


@pytest.mark.parametrize(
    "args",
    [
        ["search", "x", "--offset", "50"],
        ["search", "x", "--phrase"],
        ["search", "x", "--lang", "ru"],
        ["search", "x", "--limit", "0"],
        ["search", "x", "--limit", "10001"],
        ["search", "x" * 50],
        ["search", "x", "--from", "2026-10-32"],
        ["search", "x", "--from", "2026-10-01", "--to", "2026-09-01"],
        ["user-chats", "-100123"],
        ["posts", "https://example.com/chat"],
        ["posts", "https://t.me/+privateInvite"],
        ["posts", "https://t.me/c/123/7"],
    ],
)
def test_invalid_or_unsupported_inputs_make_no_requests(transport, args):
    requests, _ = transport
    with pytest.raises(SystemExit) as err:
        invoke(args)
    assert err.value.code != 0
    assert not requests


@pytest.mark.parametrize(
    "value", ["-10012345", "12345", "https://t.me/s/example_chat/17", "@example_chat"]
)
def test_group_identifiers_and_public_card(transport, capsys, tmp_path, value):
    requests, responses = transport
    options = []
    if "example_chat" in value:
        filename = tmp_path / "card.json"
        filename.write_text(json.dumps(public_card()), encoding="utf-8")
        options = ["--public-card", str(filename)]
    responses.append(public_card())
    out = run_cli(capsys, "group", value, *options)
    assert len(requests) == 1
    assert urlsplit(requests[-1].full_url).path == "/groups/-10012345"
    assert out["id"] == -10012345
    assert out["url"] == "https://t.me/example_chat"


def test_fresh_group_alias_never_uses_user_lookup(transport, capsys):
    requests, _ = transport
    with pytest.raises(SystemExit):
        invoke(["resolve", "@example_chat"])
    assert not requests
    assert "numeric group ID" in capsys.readouterr().err


def test_saved_alias_must_still_match_refreshed_card(transport, capsys, tmp_path):
    requests, responses = transport
    filename = tmp_path / "card.json"
    filename.write_text(json.dumps(public_card()), encoding="utf-8")
    responses.append({**public_card(), "alias": "changed_alias"})
    with pytest.raises(SystemExit):
        invoke(["resolve", "@example_chat", "--public-card", str(filename)])
    assert len(requests) == 1
    assert "alias" in capsys.readouterr().err.lower()


@pytest.mark.parametrize(
    "card",
    [
        {"id": -10012345, "is_private": True, "alias": "old_alias", "type": "CHAT"},
        {"id": -10012345, "is_private": False, "alias": "", "type": "CHAT"},
        {"id": -10012345, "alias": "example_chat", "type": "PRIVATE"},
    ],
)
def test_no_content_fetch_from_unconfirmed_public_group(transport, card):
    requests, responses = transport
    responses.append(card)
    with pytest.raises(SystemExit):
        invoke(["posts", "-10012345"])
    assert len(requests) == 1


def test_posts_null_is_empty_with_no_invented_timestamps(transport, capsys):
    requests, responses = transport
    responses.extend([public_card(), None])
    out = run_cli(capsys, "posts", "-10012345", "--limit", "10")
    assert out["posts"] == []
    assert out["status"] == "no_data"
    assert "date" not in out
    assert parse_qs(urlsplit(requests[-1].full_url).query)["members_min"] == ["0"]


def test_user_chats_requests_only_activity_panel(transport, capsys):
    requests, responses = transport
    responses.append(
        {"ok": True, "rows": [{"group_id": -10012345, "messages_count": 4}]}
    )
    out = run_cli(capsys, "user-chats", "98765", "--top", "--limit", "5")
    assert (
        urlsplit(requests[0].full_url).path == "/user_tracker/98765/panels/top_groups"
    )
    assert parse_qs(urlsplit(requests[0].full_url).query)["limit"] == ["5"]
    assert out["rows"][0]["messages_count"] == 4


def test_group_preview_explicitly_disables_processing(transport, capsys):
    requests, responses = transport
    candidate = {
        "id": -10012345,
        "alias": "example_chat",
        "name": "Example student community",
        "related_group": -10012346,
        "report": {"topics": ["part-time work"]},
    }
    responses.append(
        {
            "run_parser": False,
            "new_users_count": 0,
            "groups_count": 1,
            "groups": [candidate],
        }
    )
    out = run_cli(
        capsys, "group-preview", "student work", "--limit", "5", "--offset", "2"
    )
    assert requests[0].get_method() == "POST"
    assert urlsplit(requests[0].full_url).path == "/reporter/parse"
    params = parse_qs(urlsplit(requests[0].full_url).query)
    assert params == {
        "groups_search_query": ["student work"],
        "groups_limit": ["5"],
        "groups_offset": ["2"],
        "run_parser": ["false"],
        "find_new_users": ["false"],
        "new_users_limit": ["0"],
    }
    assert out["groups"][0] == candidate
    assert out["public_access"] == "check_group_cards"


def test_group_preview_rejects_a_response_that_claims_processing(transport):
    _, responses = transport
    responses.append({"run_parser": True, "groups": [], "new_users_count": 3})
    with pytest.raises(SystemExit):
        invoke(["group-preview", "student work"])


def test_group_preview_cannot_enable_parser_or_user_discovery(transport):
    requests, _ = transport
    with pytest.raises(SystemExit):
        invoke(["group-preview", "student work", "--run-parser"])
    assert not requests


def test_user_messages_retains_provenance_and_detects_panel_failure(transport, capsys):
    requests, responses = transport
    responses.extend(
        [
            public_card(),
            {
                "ok": True,
                "rows": [
                    {
                        "user_id": 98765,
                        "group_id": -10012345,
                        "message_url": "https://t.me/example_chat/8",
                        "text": "work",
                    },
                    {"group_id": -1009999, "text": "outside requested group"},
                ],
            },
        ]
    )
    out = run_cli(
        capsys, "user-messages", "98765", "--group", "-10012345", "--limit", "10"
    )
    assert urlsplit(requests[1].full_url).path == "/user_tracker/98765/panels/messages"
    assert out["rows"][0]["message_url"].endswith("/8")
    assert len(out["rows"]) == 1
    responses.extend([public_card(), {"ok": False, "error": "unavailable"}])
    with pytest.raises(SystemExit):
        invoke(["user-messages", "98765", "--group", "-10012345"])


def test_secrets_missing_and_errors_do_not_dump_response(
    transport, capsys, monkeypatch, tmp_path
):
    _, responses = transport
    responses.append(
        urllib.error.HTTPError(
            "https://api.vox-lab.com/search/text",
            401,
            "Unauthorized",
            {},
            io.BytesIO(b"synthetic-test-credential"),
        )
    )
    with pytest.raises(SystemExit):
        invoke(["search", "abc"])
    assert "synthetic-test-credential" not in capsys.readouterr().err
    monkeypatch.delenv("VOX_API_KEY")
    monkeypatch.setattr(vox, "KEY_FILE", tmp_path / "missing")
    with pytest.raises(SystemExit):
        invoke(["search", "abc"])


def test_single_attempt_can_be_requested_explicitly(transport):
    requests, responses = transport
    responses.append(
        urllib.error.HTTPError(
            "https://api.vox-lab.com/search/text", 503, "Unavailable", {}, io.BytesIO()
        )
    )
    with pytest.raises(SystemExit):
        invoke(["--attempts", "1", "search", "abc"])
    assert len(requests) == 1


def test_disconnect_is_retried_once_with_a_visible_diagnostic(transport, capsys):
    requests, responses = transport
    responses.extend(
        [ConnectionResetError(), {"messages": [{"id": 9, "text": "work"}], "count": 1}]
    )
    invoke(["search", "work"])
    captured = capsys.readouterr()
    assert json.loads(captured.out)["messages"][0]["id"] == 9
    assert "retry" in captured.err.lower()
    assert "synthetic-test-credential" not in captured.err
    assert len(requests) == 2
    assert requests[0].full_url == requests[1].full_url


def test_transient_retry_is_bounded(transport):
    requests, responses = transport
    responses.extend([TimeoutError(), TimeoutError()])
    with pytest.raises(SystemExit):
        invoke(["search", "work"])
    assert len(requests) == 2


def test_scoped_search_checks_public_card_and_preserves_zero_params(transport, capsys):
    requests, responses = transport
    responses.extend(
        [public_card(), {"messages": [], "count": 0, "stop_reason": "finished"}]
    )
    run_cli(
        capsys, "search", "work", "--include", "-10012345", "--exclude-users", "98765"
    )
    assert urlsplit(requests[0].full_url).path == "/groups/-10012345"
    params = parse_qs(urlsplit(requests[1].full_url).query)
    assert params["include_groups_ids"] == ["-10012345"]
    assert params["exclude_from_users"] == ["98765"]
    assert "exclude_forwards" not in params


def test_semantic_search_keeps_separate_pagination_and_decodes_report(
    transport, capsys
):
    requests, responses = transport
    responses.append(
        [{"id": -10012345, "embedding": [0.1, 0.2], "report": '{"topic":"work"}'}]
    )
    out = run_cli(capsys, "groups", "work", "--offset", "5", "--limit", "5")
    assert out["candidates"] == [{"id": -10012345, "report": {"topic": "work"}}]
    assert parse_qs(urlsplit(requests[0].full_url).query)["offset"] == ["5"]


def test_presence_caps_output_without_faking_complete_membership(transport, capsys):
    requests, responses = transport
    responses.append(
        {
            "ok": True,
            "count": 3,
            "rows": [
                {"group_id": -10012345},
                {"group_id": -10012346},
                {"group_id": -10012347, "is_private": True},
            ],
        }
    )
    out = run_cli(capsys, "user-chats", "98765", "--limit", "1")
    assert urlsplit(requests[0].full_url).path.endswith("/panels/groups")
    assert not urlsplit(requests[0].full_url).query
    assert out["rows"] == [{"group_id": -10012345}]
    assert out["count"] == 3
    assert out["locally_truncated"] is True


def test_analytics_is_labelled_and_uses_only_group_endpoint(transport, capsys):
    requests, responses = transport
    responses.extend(
        [public_card(), {"report": "synthetic summary", "embedding_big": [0.1]}]
    )
    out = run_cli(capsys, "analytics", "-10012345", "--prompt", "Which topics recur?")
    assert (
        urlsplit(requests[-1].full_url).path == "/ai_analytics/custom/GROUP/-10012345"
    )
    assert out["evidence_type"] == "ai_summary_not_primary_evidence"
    assert out["data"] == {"report": "synthetic summary"}


def test_redirect_cannot_forward_authorization_to_another_host():
    req = vox.urllib.request.Request(
        "https://api.vox-lab.com/search/text",
        headers={"Authorization": "Bearer synthetic"},
    )
    with pytest.raises(vox.VoxError, match="refusing"):
        vox.NoRedirect().redirect_request(
            req, None, 302, "Found", {}, "https://example.com/collect"
        )


def test_malformed_credential_is_rejected_without_echo_or_request(
    transport, monkeypatch, capsys
):
    requests, _ = transport
    monkeypatch.setenv("VOX_API_KEY", "synthetic-secret\nsecond-secret-line")
    with pytest.raises(SystemExit):
        invoke(["search", "work"])
    captured = capsys.readouterr()
    assert "synthetic-secret" not in captured.err + captured.out
    assert "second-secret-line" not in captured.err + captured.out
    assert not requests


def test_saved_public_card_avoids_repeating_group_lookup(transport, capsys, tmp_path):
    requests, responses = transport
    card = tmp_path / "group.json"
    card.write_text(json.dumps(public_card()), encoding="utf-8")
    responses.append(
        {"messages": [{"group_id": -10012345, "id": 8, "text": "work"}], "count": 1}
    )
    out = run_cli(
        capsys, "search", "work", "--include", "-10012345", "--public-card", str(card)
    )
    assert out["messages"][0]["id"] == 8
    assert len(requests) == 1
    assert urlsplit(requests[0].full_url).path == "/search/text"


def test_saved_card_resolves_its_alias_without_user_lookup(transport, capsys, tmp_path):
    requests, responses = transport
    card = tmp_path / "group.json"
    card.write_text(json.dumps(public_card()), encoding="utf-8")
    responses.append({"id": -10012345, "posts": ["A real sample shape"]})
    out = run_cli(
        capsys, "posts", "https://t.me/example_chat/17", "--public-card", str(card)
    )
    assert out["posts"] == ["A real sample shape"]
    assert len(requests) == 1
    assert urlsplit(requests[0].full_url).path == "/groups/-10012345/messages"


@pytest.mark.parametrize(
    "card",
    [
        {**public_card(), "id": -10099999},
        {**public_card(), "is_private": True},
        {**public_card(), "type": "PRIVATE"},
        {**public_card(), "alias": ""},
        {"id": -10012345, "alias": "example_chat", "type": "CHAT"},
    ],
)
def test_saved_card_must_confirm_public_access_for_the_same_group(
    transport, tmp_path, card
):
    requests, _ = transport
    path = tmp_path / "group.json"
    path.write_text(json.dumps(card), encoding="utf-8")
    with pytest.raises(SystemExit):
        invoke(["posts", "-10012345", "--public-card", str(path)])
    assert not requests


@pytest.mark.parametrize("limit", [None, 200, 10000])
def test_search_large_limit_preserves_messages_and_groups(transport, capsys, limit):
    requests, responses = transport
    messages = [
        {
            "id": i,
            "group_id": -10012345 if i < 150 else -10012346,
            "user_id": i % 7,
            "text": f"message {i}",
            "group_name": "Synthetic group",
            "group_url": "https://t.me/example_chat",
        }
        for i in range(200)
    ]
    responses.append({"messages": messages, "count": 999, "stop_reason": "timeout"})
    args = [] if limit is None else ["--limit", str(limit)]
    out = run_cli(capsys, "search", "подработка", *args)
    assert parse_qs(urlsplit(requests[0].full_url).query)["limit"] == [
        str(limit or 200)
    ]
    assert out["messages"] == messages
    assert out["returned_count"] == 200
    assert out["count"] == 999 and out["stop_reason"] == "timeout"
    groups = out["group_summary"]
    assert [g["message_count"] for g in groups] == [150, 50]
    assert groups[0]["group_id"] == -10012345
    assert groups[0]["unique_author_count"] == 7
    assert groups[0]["group_name"] == "Synthetic group"
    assert groups[0]["message_indices"] == list(range(150))
    assert groups[1]["message_indices"] == list(range(150, 200))
    assert len(requests) == 1  # aggregation must not trigger more API calls


def test_search_summary_counts_only_retained_messages(transport, capsys):
    _, responses = transport
    responses.append(
        {
            "messages": [
                {"id": 1, "group_id": -10012345, "user_id": 42},
                {"id": 2, "group_id": -10012345, "user_id": 42},
                {"id": 3, "group_id": -10012345},
                {"id": 4, "group_id": -10099999, "is_private": True},
                {"id": 5, "text": "no group id"},
            ]
        }
    )
    out = run_cli(capsys, "search", "work")
    assert len(out["group_summary"]) == 1
    assert out["group_summary"][0]["message_count"] == 3
    assert out["group_summary"][0]["unique_author_count"] == 1
    assert out["ungrouped_count"] == 1
    assert out["returned_count"] == 4
