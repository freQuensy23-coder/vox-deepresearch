"""Offline contracts for the telegram-research scripts (vox.py, jev.py, read.py, notes.py, facts.py, tme.py)."""

import importlib.util
import io
import json
import sys
import urllib.error
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[2] / "skills/public/telegram-research/scripts"
sys.path.insert(0, str(SCRIPTS))  # the scripts import pool.py from their own folder


def load(name):
    spec = importlib.util.spec_from_file_location(f"telegram_research_{name}", SCRIPTS / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


vox = load("vox")
jev = load("jev")
read = load("read")
notes = load("notes")
facts = load("facts")
pool = load("pool")
tme = load("tme")


def row(mid, date, group=-1001, text=None, user=7):
    return {"id": mid, "group_id": group, "date": date, "text": text or f"text {mid}", "user_id": user, "user_alias": f"u{user}", "message_url": f"https://t.me/g/{mid}"}


@pytest.fixture(autouse=True)
def research_folder(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    return tmp_path


def raw():
    return list(pool.rows())


def put(messages):
    for message in messages:
        for source in message.pop("sources"):
            pool.add([(vox._key(message), message)], source)


def test_full_window_is_split_until_complete_and_gaps_are_recovered(monkeypatch):
    monkeypatch.setattr(vox, "PAGE", 3)
    corpus = [row(i, f"2026-01-{d:02d}T00:00:00Z") for i, d in enumerate([30, 28, 20, 15, 10, 5, 2], start=1)]
    calls = []

    def fake(path, params):
        calls.append((params["from_date"], params["to_date"]))
        inside = [m for m in corpus if params["from_date"] <= m["date"] <= params["to_date"]]
        # Like Vox: a full response holds the newest and oldest matches, with a gap between.
        page = inside if len(inside) < 3 else [inside[0], inside[1], inside[-1]]
        return {"stop_reason": "finished", "messages": page}

    monkeypatch.setattr(vox, "_call", fake)
    vox.Vox().add_keywords("вахта", source="seed")
    vox.Vox().search("вахта", since="2026-01-01", until="2026-02-01")
    assert sorted(m["id"] for m in raw()) == [1, 2, 3, 4, 5, 6, 7]
    assert calls[0] == ("2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z")
    assert raw()[0]["sources"] == ["keyword:вахта"]
    assert json.loads(Path("registry.json").read_text())["keywords"][0]["status"] == "searched"


def test_search_resumes_pending_windows_after_failure(monkeypatch):
    monkeypatch.setattr(vox, "PAGE", 2)
    responses = [{"stop_reason": "finished", "messages": [row(5, "2026-01-20T00:00:00Z"), row(4, "2026-01-02T00:00:00Z")]}, vox.VoxError("boom")]

    def flaky(path, params):
        item = responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(vox, "_call", flaky)
    with pytest.raises(vox.VoxError):
        vox.Vox().search("вахта", since="2026-01-01", until="2026-02-01")
    assert sorted(m["id"] for m in raw()) == [4, 5]  # pooled before the failure
    seen = []
    monkeypatch.setattr(vox, "_call", lambda path, params: seen.append((params["from_date"], params["to_date"])) or {"stop_reason": "finished", "messages": [row(3, "2026-01-10T00:00:00Z")]})
    vox.Vox().search("вахта", since="2026-01-01", until="2026-02-01")
    assert len(seen) == 2  # the two halves left after the failure, not the whole window again
    assert sorted(m["id"] for m in raw()) == [3, 4, 5]


def test_whale_messages_join_the_pool_deduplicated(monkeypatch):
    monkeypatch.setattr(vox, "_call", lambda path, params: {"stop_reason": "finished", "messages": [row(4, "2026-01-07T00:00:00Z", user=42)]})
    vox.Vox().search("вахта", since="2026-01-01", until="2026-02-01")
    calls = []
    panel = {"rows": [
        {"date": "2026-01-07T00:00:00Z", "group_id": -1001, "message_url": "https://t.me/g/4", "text": "text 4"},
        {"date": "2026-01-08T00:00:00Z", "group_id": -2002, "message_url": "https://t.me/h/9", "text": "elsewhere"},
        {"date": "2025-06-01T00:00:00Z", "group_id": -2002, "message_url": "https://t.me/h/1", "text": "too old"},
    ]}
    monkeypatch.setattr(vox, "_call", lambda path, params: calls.append((path, params)) or panel)
    vox.Vox().add_whale(42, alias="boss", reason="leader")
    vox.Vox().whale(42, since="2026-01-01")
    assert calls == [("/user_tracker/42/panels/messages", {"limit": 100000})]
    pool = {(m["group_id"], m["id"]): m for m in raw()}
    assert set(pool) == {(-1001, 4), (-2002, 9)}
    assert pool[(-1001, 4)]["sources"] == ["keyword:вахта", "whale:42"]
    assert pool[(-2002, 9)]["user_id"] == 42
    assert json.loads(Path("registry.json").read_text())["whales"][0]["status"] == "fetched"


def test_registry_skips_known_keywords():
    vox.Vox().add_keywords("вахта", "Вахта", "межвахта", source="seed")
    assert [k["word"] for k in json.loads(Path("registry.json").read_text())["keywords"]] == ["вахта", "межвахта"]


def test_authors_counts_filtered_messages():
    Path("messages_filtered.json").write_text(json.dumps({"messages": [row(1, "d", user=1), row(2, "d", group=-2, user=1), row(3, "d", user=2)]}))
    data = json.loads(Path(vox.Vox().authors(top=5)).read_text())
    assert [(a["user_id"], a["messages"], a["groups"]) for a in data["authors"]] == [(1, 2, 2), (2, 1, 1)]


def test_proxy_timeout_surfaces_its_advice(monkeypatch):
    body = json.dumps({"error": "request_timeout", "message": "Request exceeded 250 seconds. The query is too heavy: make it more specific."}).encode()

    class Opener:
        def open(self, request, timeout):
            raise urllib.error.HTTPError(request.full_url, 504, "timeout", {}, io.BytesIO(body))

    monkeypatch.setenv("VOX_RELAY_URL", "http://127.0.0.1:1")
    monkeypatch.setattr(vox, "_OPENER", Opener())
    with pytest.raises(vox.VoxError, match="more specific"):
        vox._call("/search/text", {"query": "вахта"})


def test_relay_is_required(monkeypatch):
    monkeypatch.delenv("VOX_RELAY_URL", raising=False)
    with pytest.raises(vox.VoxError, match="VOX_RELAY_URL"):
        vox._call("/search/text", {"query": "вахта"})


def test_jev_batches_respect_size_limit(monkeypatch):
    monkeypatch.setattr(jev, "MAX_BYTES", 2000)
    unique = {f"t{i}": {"text": "x" * 300} for i in range(10)}
    chunks = list(jev._chunks(list(unique), unique))
    assert sum(len(c) for c in chunks) == 10 and all(len(c) <= 2 for c in chunks)


def test_jev_batches_count_the_instruction_in_every_item(monkeypatch):
    monkeypatch.setattr(jev, "MAX_BYTES", 20000)
    unique = {f"t{i}": {"text": "x"} for i in range(100)}
    chunks = list(jev._chunks(list(unique), unique, overhead=1000))
    assert all(len(c) <= 10 for c in chunks)  # a long instruction is repeated per item


def test_jev_filter_dedups_texts_caches_scores_and_samples(monkeypatch):
    put([
        {**row(1, "d", text="Опыт  вахты"), "sources": ["keyword:вахта"]},
        {**row(2, "d", group=-2, text="опыт вахты"), "sources": ["whale:7"]},
        {**row(3, "d", text="реклама"), "sources": ["keyword:вахта"]},
    ])
    submitted = []
    monkeypatch.setattr(jev, "submit", lambda data: submitted.append(data) or {"id": "batch_x", "texts": {i["id"]: i["state"] for i in data["items"]}})
    monkeypatch.setattr(jev, "collect", lambda handle: {"complete": True, "results": [{"custom_id": k, "response": {"answers": {"relevant": {"noul": 0.9 if "пыт" in t else 0.1}}}} for k, t in handle["texts"].items()]})
    jev.Jev().filter("Личный опыт?")
    filtered = json.loads(Path("messages_filtered.json").read_text())
    assert [(m["message_id"], m["duplicates"], m["sources"]) for m in filtered["messages"]] == [(1, 1, ["keyword:вахта", "whale:7"])]
    assert {"key", "group_id", "message_url", "date", "user_id", "text", "jev_score"} <= set(filtered["messages"][0])
    assert len(submitted[0]["items"]) == 2
    jev.Jev().filter("Личный опыт?")
    assert len(submitted) == 1  # scores are cached per instruction
    lines = jev.Jev().sample(n=10).splitlines()
    assert lines[0].startswith("+ 0.90") and lines[1].startswith("- 0.10")


def test_checkpoint_keeps_only_the_windows_left(monkeypatch):
    monkeypatch.setattr(vox, "_call", lambda path, params: {"stop_reason": "finished", "messages": [row(8, "2026-01-05T00:00:00Z")]})
    vox.Vox().search("вахта", since="2026-01-01", until="2026-02-01")
    state = json.loads(Path("vox/search/вахта_2026-01-01_2026-02-01.json").read_text())
    assert state == {"query": "вахта", "since": "2026-01-01T00:00:00Z", "until": "2026-02-01T00:00:00Z", "kind": "all", "pending": [], "calls": 1, "complete": True}
    assert [m["id"] for m in raw()] == [8]


def test_full_proxy_queue_tells_the_agent_to_do_other_work(monkeypatch):
    body = json.dumps({"error": "queue_full", "message": "Vox queue is full: 100 requests are already waiting. Do other work meanwhile"}).encode()

    class Opener:
        def open(self, request, timeout):
            raise urllib.error.HTTPError(request.full_url, 503, "busy", {}, io.BytesIO(body))

    monkeypatch.setenv("VOX_RELAY_URL", "http://127.0.0.1:1")
    monkeypatch.setattr(vox, "_OPENER", Opener())
    with pytest.raises(vox.VoxError, match="Do other work"):
        vox._call("/search/text", {"query": "вахта"})


def test_registry_shows_keyword_yield():
    vox.Vox().add_keywords("вахта", "межвахта", source="seed")
    put([{**row(1, "d"), "sources": ["keyword:вахта"]}, {**row(2, "d"), "sources": ["keyword:вахта", "whale:7"]}])
    Path("messages_filtered.json").write_text(json.dumps({"messages": [{"sources": ["keyword:вахта"]}]}))
    keywords = json.loads(vox.Vox().registry())["keywords"]
    assert [(k["word"], k["raw"], k["on_topic"]) for k in keywords] == [("вахта", 2, 1), ("межвахта", 0, 0)]


def filtered_file(messages, **meta):
    Path("messages_filtered.json").write_text(json.dumps({"instruction": "q", "threshold": 0.5, "unique_texts": len(messages), **meta, "messages": messages}))


def fm(mid, text, group=-1001, date="2026-03-01", duplicates=0, score=0.9):
    return {**row(mid, date, group=group, text=text), "group_name": f"Chat {group}", "group_alias": f"c{abs(group)}", "message_id": mid, "duplicates": duplicates, "jev_score": score, "sources": []}


def test_read_collapses_near_duplicates_and_filters():
    filtered_file([
        fm(1, "Вахта в Сургуте 80000 рублей, звоните +7 900 111 22 33"),
        fm(2, "вахта в сургуте 80000 рублей, звоните +7 900 999 88 77", group=-2, date="2026-03-05", duplicates=3),
        fm(3, "Кормят нормально, жильё в вагончике"),
    ])
    out = read.read(grep="вахт")
    assert "×5 в 2 чатах" in out and "https://t.me/g/1" in out and "Кормят" not in out
    assert "-- 1–1 of 1" in out
    assert read.read(count=True).splitlines()[0].startswith("2 entries (3 messages")
    assert "Кормят" not in read.read(since="2026-03-05") and "Chat -2" not in read.read(group="Chat -1001")


def test_read_pages_fit_the_tool_output_limit_and_chunks_cover_everything():
    filtered_file([fm(i, "слово" + "я" * i + " " + "текст " * 150, date=f"2026-03-{i:02d}") for i in range(1, 21)])
    first = read.read(max_chars=3000)
    assert len(first) < 3500 and "next page: --offset" in first
    nxt = int(first.rsplit("--offset ", 1)[1])
    assert f"#{nxt + 1} " in read.read(offset=nxt, max_chars=3000)
    assert read.read(chunks=True).splitlines() == ["--offset 0 --end 20"]
    tail = read.read(offset=16, end=20, max_chars=100000)
    assert "#17 " in tail and "#20 " in tail and "range up to 20 read to the end" in tail


def test_notes_status_count_and_show(monkeypatch):
    filtered_file([fm(i, "текст" + "я" * i) for i in range(1, 6)])
    Path("verticals.md").write_text("01-pay | ставки?\n02-food | питание?\n")
    Path("notes").mkdir()
    Path("notes/0.md").write_text("01-pay | 80 тыс за смену | https://t.me/g/1\n02-food | кормят трижды | https://t.me/g/2\n# done 0-3\n")
    Path("notes/3.md").write_text("01-pay | 90 тыс | https://t.me/g/4\nother | x | https://t.me/g/3\nmisc | что-то | https://t.me/g/5\n# upto 4\n")
    monkeypatch.setattr(notes.read, "CHUNK", 3)
    status = notes.Notes().status()
    assert status.splitlines()[0] == "1 of 2 chunks done (5 entries)"
    assert "not done: --offset 3 --end 5 (read up to 4)" in status and "unknown vertical 'misc': 1 notes" in status
    assert notes.Notes().count().splitlines() == ["     2  01-pay", "     1  02-food"]
    shown = notes.Notes().show("01-pay")
    assert "1. 80 тыс за смену | https://t.me/g/1" in shown and "2. 90 тыс" in shown and "-- 1–2 of 2" in shown
    found = notes.Notes().grep("кормят|90")
    assert "[02-food] кормят трижды" in found and "[01-pay] 90 тыс" in found and "-- 1–2 of 2" in found


def test_facts_lists_every_group_with_links_and_counts():
    put([{**row(i, "2026-03-0%d" % i, group=-1 - i % 2, text=f"t{i} https://hh.ru/v{i} @boss_ivan +7 900 111 22 3{i}"), "sources": ["keyword:вахта"]} for i in range(1, 5)])
    filtered_file([fm(1, "Вахта https://hh.ru/v1 @boss_ivan +7 900 111 22 31", group=-2, duplicates=2), fm(2, "Ставка t.me/job_chat", group=-1), fm(3, "Ещё про вахту", group=-2)], unique_texts=4)
    Path("registry.json").write_text(json.dumps({"keywords": [{"word": "вахта", "status": "searched"}, {"word": "смена", "status": "todo"}], "whales": []}))
    text = facts.facts()
    assert "| Групп в пуле / по теме | 2 / 2 |" in text and "| Ключевых слов: найдено / всего | 1 / 2 |" in text
    assert "| 1 | Chat -2 | https://t.me/g | 2 | 4 | 2 |" in text
    assert "| Уникальных @-контактов | 1 |" in text and "hh.ru | 1" in text
    assert "+7 900" not in text and "@boss_ivan" not in text
    assert facts.facts(out="report/facts.md").endswith("2 groups") and Path("report/facts.md").exists()


# tme.py: synthetic pages shaped like t.me's public widgets (channel listing, message embed, discussion).


def tme_channel(name, posts):
    """/s/NAME listing: POSTS are (id, text) of a channel."""
    items = "".join(
        f'<div class="tgme_widget_message_wrap js-widget_message_wrap"><div class="tgme_widget_message js-widget_message" data-post="{name}/{mid}">'
        f'<div class="tgme_widget_message_bubble"><div class="tgme_widget_message_author accent_color"><a class="tgme_widget_message_owner_name" href="https://t.me/{name}"><span dir="auto">Вахта Север</span></a></div>'
        f'<div class="tgme_widget_message_photo_wrap"><i class="tgme_widget_message_photo"></i></div>'
        f'<div class="tgme_widget_message_text js-message_text" dir="auto">{text}</div>'
        f'<div class="tgme_widget_message_footer"><span class="tgme_widget_message_views">1K</span><span class="tgme_widget_message_meta">'
        f'<a class="tgme_widget_message_date" href="https://t.me/{name}/{mid}"><time datetime="2026-05-0{mid % 9 + 1}T10:00:00+00:00" class="time">10:00</time></a></span></div></div></div></div>'
        for mid, text in posts
    )
    return f'<html><body><div class="tgme_channel_history js-message_history">{items}</div></body></html>'


def tme_embed(name, mid, text, author="Иван", reply=None):
    """?embed=1 page of one chat message; REPLY is the id it answers."""
    quote = (
        f'<a class="tgme_widget_message_reply" href="https://t.me/{name}/{reply}" "><div class="tgme_widget_message_author accent_color">'
        f'<span class="tgme_widget_message_author_name" dir="auto">Пётр</span></div><div class="tgme_widget_message_text js-message_reply_text" dir="auto">цитата {reply}</div></a>'
        if reply
        else ""
    )
    return (
        f'<html><body><div class="tgme_widget_message js-widget_message" data-post="{name}/{mid}"><div class="tgme_widget_message_bubble">'
        f'<div class="tgme_widget_message_author accent_color"><a class="tgme_widget_message_author_name" href="https://t.me/private_handle"><span dir="auto">{author}</span></a>&nbsp;in&nbsp;'
        f'<a class="tgme_widget_message_owner_name" href="https://t.me/{name}"><span dir="auto">Чат вахтовиков</span></a></div>{quote}'
        f'<div class="tgme_widget_message_text js-message_text" dir="auto">{text}</div>'
        f'<div class="tgme_widget_message_footer"><a class="tgme_widget_message_date" href="https://t.me/{name}/{mid}"><time datetime="2026-05-01T10:0{mid % 10}:00+00:00">May 1</time></a></div>'
        "</div></div></body></html>"
    )


MISSING = '<html><body><div class="tgme_widget_message"><div class="tgme_widget_message_error" dir="auto">Post not found</div></div></body></html>'


def tme_discussion(name, post, total, comments):
    """?discussion=1 page: the latest COMMENTS (id, reply id or None, text) of TOTAL under NAME/POST."""
    items = "".join(
        f'<div class="tgme_widget_message_wrap js-widget_message_wrap"><div class="tgme_widget_message js-widget_message" data-peer="c1_2" data-post-id="{cid}">'
        f'<div class="tgme_widget_message_bubble"><div class="tgme_widget_message_author accent_color"><span class="tgme_widget_message_author_name"><span dir="auto">Ольга</span></span></div>'
        + (
            f'<div class="tgme_widget_message_reply js-reply_to" data-reply-to="{reply}"><div class="tgme_widget_message_author"><span class="tgme_widget_message_author_name">Ян</span></div>'
            f'<div class="tgme_widget_message_text js-message_reply_text">ответ {reply}</div></div>'
            if reply
            else ""
        )
        + f'<div class="tgme_widget_message_reply_template js-reply_tpl"><input type="hidden" name="reply_to_id" value="{cid}"><div class="tgme_widget_message_author"><span class="tgme_widget_message_author_name">Ольга</span></div>'
        f'<div class="tgme_widget_message_text js-message_reply_text">{text}</div></div><div class="tgme_widget_message_text js-message_text" dir="auto">{text}</div>'
        f'<div class="tgme_widget_message_footer"><a class="tgme_widget_message_date" href="https://t.me/{name}/{post}?comment={cid}"><time datetime="2026-05-02T09:00:00+00:00">May 2</time></a></div></div></div></div>'
        for cid, reply, text in comments
    )
    return f'<html><body><h3 class="tgme_post_discussion_header"><span class="js-header">{total} comments</span> on this post</h3><div class="tgme_post_discussion">{items}</div></body></html>'


@pytest.fixture
def tme_pages(monkeypatch):
    """Serve PAGES (t.me path -> html, or (status, location)) instead of the proxy; record fetched paths."""
    pages, fetched = {}, []

    def fake(path):
        fetched.append(path)
        page = pages.get(path, MISSING)
        return (page[0], page[1], "") if isinstance(page, tuple) else (200, None, page)

    monkeypatch.setattr(tme, "_fetch", fake)
    return pages, fetched


def test_tme_reads_channel_posts_chat_messages_and_comments():
    posts, missing, _ = tme.parse_page(tme_channel("vahta_sever", [(7, "Набор на вахту: 60/30, <b>4500</b> за смену.<br/>Пиши @vahta_hr <a href=\"https://t.me/vahta_hr\">@vahta_hr</a>, подробности <a href=\"https://t.me/vahta_chat/90\">тут</a>")]))
    assert not missing and posts[0]["id"] == 7 and posts[0]["author"] == "Вахта Север" and posts[0]["media"] == ["фото"]
    assert posts[0]["text"].startswith("Набор на вахту: 60/30, 4500 за смену.\nПиши")
    assert "тут <https://t.me/vahta_chat/90>" in posts[0]["text"] and "<https://t.me/vahta_hr>" not in posts[0]["text"]
    message = tme.parse_page(tme_embed("vahta_chat", 91, "Мне так и не заплатили", reply=90))[0][0]
    assert message["author"] == "Иван" and message["reply_to"] == "vahta_chat/90" and message["reply_text"] == "цитата 90"
    assert message["text"] == "Мне так и не заплатили" and "private_handle" not in tme.render(message, "vahta_chat")
    hidden = tme.parse_page(tme_embed("vahta_chat", 92, "Тоже ждём", author=""))[0][0]
    assert hidden["author"] == "автор скрыт"
    comments, _, header = tme.parse_page(tme_discussion("vahta_sever", 7, 3, [(501, None, "Кто ездил?"), (502, 501, "Я, расчёт задержали")]))
    assert header == "3 comments" and [c["id"] for c in comments] == [501, 502] and comments[1]["reply_to"] == "#501"
    assert comments[1]["text"] == "Я, расчёт задержали" and comments[1]["url"].endswith("?comment=502")
    assert tme.parse_page(MISSING)[1] is True


@pytest.mark.parametrize("url", ["https://t.me/c/1234567/89", "https://t.me/c2356977124/11415", "https://t.me/+AbCdEf123", "t.me/joinchat/AbCdEf", "https://example.com/vahta_chat/1", "https://t.me/vahta_chat"])
def test_tme_refuses_private_and_non_message_links(url):
    with pytest.raises(tme.TmeError):
        tme.parse_url(url)


def test_tme_accepts_public_message_links():
    assert tme.parse_url("https://t.me/vahta_chat/91") == ("vahta_chat", 91, None)
    assert tme.parse_url("t.me/s/vahta_sever/7") == ("vahta_sever", 7, None)
    assert tme.parse_url("https://t.me/vahta_chat/12/91") == ("vahta_chat", 91, None)
    assert tme.parse_url("https://t.me/vahta_sever/7?comment=502") == ("vahta_sever", 7, 502)


def test_tme_show_chat_opens_neighbours_once(tme_pages):
    pages, fetched = tme_pages
    pages["/s/vahta_chat?before=94"] = (302, "/vahta_chat")
    for mid in (89, 91, 92):
        pages[f"/vahta_chat/{mid}?embed=1"] = tme_embed("vahta_chat", mid, f"сообщение {mid}")
    shown = tme.Tme().show("https://t.me/vahta_chat/91", before=2, after=2)
    assert shown.startswith("vahta_chat (чат) · #89–#93")
    assert "▶ #91 · 2026-05-01 10:01 · Иван" in shown and "#90 · нет: удалено" in shown and "сообщение 92" in shown
    first = len(fetched)
    assert tme.Tme().show("https://t.me/vahta_chat/91", before=2, after=2) == shown and len(fetched) == first
    pages["/vahta_chat/94?embed=1"] = tme_embed("vahta_chat", 94, "сообщение 94")
    tme.Tme().show("https://t.me/vahta_chat/94", before=0, after=0)
    assert fetched[first:] == ["/vahta_chat/94?embed=1"]  # known chat: no second /s/ probe
    pages["/s/vahta_chats?before=2"] = tme_channel("vahta_chats", [(1, "канал с похожим именем")])
    assert "канал с похожим именем" in tme.Tme().show("https://t.me/vahta_chats/1", before=0, after=0)


def test_tme_show_channel_scrolls_listing_pages(tme_pages):
    pages, fetched = tme_pages
    pages["/s/vahta_sever?before=31"] = tme_channel("vahta_sever", [(i, f"пост {i}") for i in range(11, 31)])
    pages["/s/vahta_sever?before=11"] = tme_channel("vahta_sever", [(i, f"пост {i}") for i in range(1, 11)])
    shown = tme.Tme().show("https://t.me/vahta_sever/20", before=12, after=10)
    assert shown.startswith("vahta_sever (канал) · #8–#30") and "▶ #20" in shown and "пост 8" in shown and "пост 30" in shown and "пост 7" not in shown
    assert fetched == ["/s/vahta_sever?before=31", "/s/vahta_sever?before=11"]


def test_tme_thread_follows_replies_up_to_the_first_message(tme_pages):
    pages, _ = tme_pages
    pages["/vahta_chat/95?embed=1"] = tme_embed("vahta_chat", 95, "Суд выиграли", reply=80)
    pages["/vahta_chat/80?embed=1"] = tme_embed("vahta_chat", 80, "Подали в суд", reply=12)
    pages["/vahta_chat/12?embed=1"] = tme_embed("vahta_chat", 12, "Нам не платят третий месяц")
    shown = tme.Tme().thread("https://t.me/vahta_chat/95")
    assert shown.index("Нам не платят") < shown.index("Подали в суд") < shown.index("Суд выиграли")
    assert "▶ #95" in shown and "3 сообщ." in shown


def test_tme_comments_point_to_the_discussion_chat_for_older_ones(tme_pages):
    pages, _ = tme_pages
    pages["/vahta_sever/7?embed=1&discussion=1&comments_limit=100"] = tme_discussion("vahta_sever", 7, 250, [(501, None, "Кто ездил?"), (502, 501, "Я")])
    pages["/vahta_sever/7?embed=1&comment=501"] = tme_embed("vahta_sever_chat", 501, "Кто ездил?")
    shown = tme.Tme().comments("https://t.me/vahta_sever/7")
    assert shown.startswith("vahta_sever/7: 250 comments") and "↩ #501 «ответ 501»" in shown
    assert "показаны последние 2 из 250" in shown and "tme.py show https://t.me/vahta_sever_chat/501 --before 20" in shown


def test_tme_page_says_a_chat_has_no_listing(tme_pages):
    pages, _ = tme_pages
    pages["/s/vahta_chat"] = (302, "/vahta_chat")
    assert "чат, а не канал" in tme.Tme().page("vahta_chat")


def test_tme_proxy_is_required_and_local(monkeypatch):
    monkeypatch.delenv("TME_RELAY_URL", raising=False)
    with pytest.raises(tme.TmeError, match="TME_RELAY_URL"):
        tme._relay()
    monkeypatch.setenv("TME_RELAY_URL", "https://t.me")
    with pytest.raises(tme.TmeError, match="local"):
        tme._relay()


def test_tme_proxy_advice_reaches_the_agent(monkeypatch):
    monkeypatch.setenv("TME_RELAY_URL", "http://127.0.0.1:9")
    body = io.BytesIO(json.dumps({"error": "refused", "message": "t.me keeps refusing requests from this host"}).encode())

    def refuse(request, timeout=None):
        raise urllib.error.HTTPError(request.full_url, 503, "busy", {}, body)

    monkeypatch.setattr(tme._OPENER, "open", refuse)
    with pytest.raises(tme.TmeError, match="keeps refusing"):
        tme._fetch("/vahta_chat/1?embed=1")


def test_tme_never_points_to_a_private_discussion_chat(tme_pages):
    pages, fetched = tme_pages
    pages["/vahta_sever/7?embed=1&discussion=1&comments_limit=100"] = tme_discussion("vahta_sever", 7, 250, [(501, None, "Кто ездил?"), (502, 501, "Я, расчёт задержали"), (503, 502, "Подавай в инспекцию")])
    pages["/vahta_sever/7?embed=1&comment=501"] = tme_embed("c2356977124", 501, "Кто ездил?")
    pages["/vahta_sever/7?embed=1&comment=503"] = tme_embed("c2356977124", 503, "Подавай в инспекцию", reply=502)
    shown = tme.Tme().comments("https://t.me/vahta_sever/7")
    assert "чат обсуждения закрыт" in shown and "c2356977124" not in shown
    chain = tme.Tme().thread("https://t.me/vahta_sever/7?comment=503")
    assert chain.index("Кто ездил?") < chain.index("расчёт задержали") < chain.index("▶ #503")
    assert not any("c2356977124" in path for path in fetched)


def test_facts_never_link_a_chat_without_a_public_name():
    put([{**row(1, "2026-03-01", group=-5), "sources": ["keyword:вахта"]}])
    filtered_file([{**fm(1, "Вахта в закрытом чате", group=-5), "group_url": "https://t.me/c/1001676657599", "message_url": "https://t.me/c/1001676657599/77"}])
    text = facts.facts()
    assert "t.me/c/" not in text and "нет публичной ссылки" in text
