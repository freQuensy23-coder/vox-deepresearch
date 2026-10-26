"""Check a research report against the report form before it is shown.

    check.py /mnt/user-data/outputs/report.md        # first report
    check.py /mnt/user-data/outputs/deep-report.md   # deep report

Run from the research folder: the check reads report/facts.md, questions.md and,
for the deep report, deep/stories/. It prints every problem as `where: what`, or
OK. The lead agent's turn cannot end while a report fails it.

The form: the facts table (report/facts.md, unchanged, nothing before it), then
`## Общее резюме`, then one `## ` section per questions.md entry, in its order.
No sources list, no file paths, no hedges outside «Чего не видно в данных», no
words glued to numbers. The deep report also links every story the
investigators wrote to deep/stories/, at least MIN_STORIES of them.
"""

import re
import sys
from pathlib import Path

FACTS = Path("report/facts.md")
QUESTIONS = Path("questions.md")
STORIES = Path("deep/stories")
SUMMARY = "Общее резюме"
UNSEEN = "Чего не видно в данных"
MIN_SECTIONS = 10
MIN_STORIES = 100
WORDS = {"report": (1200, 2500), "deep": (1500, 3500)}
QUESTION = re.compile(r"^## \S+ \| (.+?)\s*$", re.MULTILINE)
HEADING = re.compile(r"^(#{1,6}) +(.+?)\s*$", re.MULTILINE)
URL = re.compile(r"https?://[^\s)\]>]+")
TME = re.compile(r"https?://t\.me/[^\s)\]>]+", re.IGNORECASE)
FORBIDDEN_HEADINGS = {"источники", "список источников", "sources", "литература", "ссылки", "метод", "методология", "о методе"}
PATH = re.compile(r"/mnt/|user-data|\b[\w-]+\.(?:json|md|db|py|txt)\b|\b(?:notes|sections|deep|report|stories)/")
GLUED = re.compile(r"[А-Яа-яЁё]\d|\d[А-Яа-яЁё]{3,}")
HEDGES = re.compile(
    "|".join(
        [
            r"не доказыва",
            r"не подтвержд",
            r"нельзя (?:считать|вывести|превращать|называть|описывать|обобщать|утверждать)",
            r"из этого не следует",
            r"не означает",
            r"не явля[ею]тся",
            r"не равн(?:о|означ)",
            r"самоотч[её]т",
            r"заинтересованн\w* (?:сторон|свидетельств)",
            r"возраст\w*[^.\n]{0,40}не (?:установлен|проверен|подтвержд)",
            r"совершеннолети\w*[^.\n]{0,60}не (?:установлен|устанавлива|проверя|подтвержд)",
            r"доказанн\w*",
            r"следует отметить",
            r"в рамках",
            r"осуществля\w*",
            r"\bданн(?:ый|ая|ое|ого|ой|ому|ом|ую)\b",
        ]
    ),
    re.IGNORECASE,
)


def _clean(text):
    """Text without URLs: links are not words, and their slugs hold digits and dots."""
    return URL.sub(" ", text)


def _blocks(text, level):
    """[(title, body)] of the headings of exactly LEVEL in TEXT, each up to the next heading of that level or higher."""
    marks = [m for m in HEADING.finditer(text) if len(m.group(1)) <= level]
    return [(m.group(2), text[m.end() : marks[i + 1].start() if i + 1 < len(marks) else len(text)]) for i, m in enumerate(marks) if len(m.group(1)) == level]


def _hedges(where, text):
    """Hedges and glued numbers in TEXT, except in the «Чего не видно в данных» part."""
    found = []
    for line in _clean(text.split(f"### {UNSEEN}")[0]).splitlines():
        for match in HEDGES.finditer(line):
            found.append(f"{where}: оговорка «{match.group(0)}» в строке: {line.strip()[:160]}")
        for match in GLUED.finditer(line):
            found.append(f"{where}: слово слиплось с числом «{match.group(0)}» в строке: {line.strip()[:160]}")
    return found


def _section(kind, title, body):
    where = f"раздел «{title}»"
    found = _hedges(where, body)
    low, high = WORDS[kind]
    words = len(_clean(body).split())
    if not low <= words <= high:
        found.append(f"{where}: {words} слов, нужно {low}–{high}")
    unseen = [b for t, b in _blocks(body, 3) if t == UNSEEN]
    if not unseen:
        found.append(f"{where}: нет подраздела «### {UNSEEN}»")
    elif len(re.findall(r"^\s*(?:[-*]|\d+\.) ", unseen[0], re.MULTILINE)) > 3:
        found.append(f"{where}: в «{UNSEEN}» больше трёх пунктов")
    return found


def _stories(folder, text):
    """Each story in deep/stories/*.md is a `## ` heading with t.me links; every story must be linked from the report."""
    found, total = [], 0
    linked = {u.rstrip(".,;:").lower() for u in TME.findall(text)}
    for path in sorted((folder / STORIES).glob("*.md")):
        for title, body in _blocks(path.read_text(encoding="utf-8"), 2):
            total += 1
            links = {u.rstrip(".,;:").lower() for u in TME.findall(body)}
            if not links:
                found.append(f"{STORIES / path.name}: у истории «{title}» нет ссылок на t.me")
            elif not links & linked:
                found.append(f"история «{title}» ({STORIES / path.name}) не упомянута в отчёте ни одной своей ссылкой")
    if total < MIN_STORIES:
        found.insert(0, f"разобрано историй: {total}, нужно не меньше {MIN_STORIES}")
    return found


def problems(report, folder="."):
    """Every way REPORT breaks the form; [] when it passes. FOLDER is the research folder."""
    report, folder = Path(report), Path(folder)
    kind = "deep" if report.name == "deep-report.md" else "report"
    if not report.is_file():
        return [f"{report.name}: файла нет"]
    if not (folder / FACTS).is_file():
        return [f"{FACTS} нет: python3 $V/facts.py --out {FACTS}"]
    text = report.read_text(encoding="utf-8")
    facts = (folder / FACTS).read_text(encoding="utf-8").strip()
    found = []
    if not text.lstrip().startswith(facts):
        found.append(f"начало отчёта: первым идёт {FACTS} без изменений, без заголовка и текста перед ним")
        rest = text
    else:
        rest = text.lstrip()[len(facts) :]
    for match in HEADING.finditer(rest):
        if match.group(1) == "#":
            found.append(f"заголовок «# {match.group(2)}»: заголовков первого уровня в отчёте нет")
        elif match.group(2).strip(" :").casefold() in FORBIDDEN_HEADINGS:
            found.append(f"заголовок «{match.group(2)}»: разделов об источниках и методе в отчёте нет")
    for line in _clean(rest).splitlines():
        if PATH.search(line):
            found.append(f"путь к файлу в строке: {line.strip()[:160]}")
    blocks = _blocks(rest, 2)
    if not blocks or blocks[0][0] != SUMMARY:
        found.append(f"после таблицы фактуры идёт «## {SUMMARY}»")
    else:
        summary = blocks.pop(0)[1].strip()
        found += _hedges(f"«{SUMMARY}»", summary.rsplit("\n\n", 1)[0])
    expected = QUESTION.findall((folder / QUESTIONS).read_text(encoding="utf-8")) if (folder / QUESTIONS).is_file() else []
    if kind == "report" and len(expected) < MIN_SECTIONS:
        found.append(f"{QUESTIONS}: {len(expected)} разделов, нужно 10–15 по исходному вопросу плюс разделы, которые назвал пользователь")
    titles = [title for title, _ in blocks]
    if titles != expected:
        missing, extra = [t for t in expected if t not in titles], [t for t in titles if t not in expected]
        found += [f"раздела «{t}» из {QUESTIONS} нет в отчёте" for t in missing] + [f"раздел «{t}»: его нет в {QUESTIONS}" for t in extra]
        if not missing and not extra:
            found.append(f"разделы идут не в порядке {QUESTIONS}")
    for title, body in blocks:
        found += _section(kind, title, body)
    if kind == "deep":
        found += _stories(folder, text)
    return found


def main():
    if len(sys.argv) != 2:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    found = problems(sys.argv[1])
    print("\n".join(found) if found else "OK")
    sys.exit(1 if found else 0)


if __name__ == "__main__":
    main()
