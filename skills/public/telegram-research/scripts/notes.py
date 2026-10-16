"""Readers' notes for the final report: who read which chunk, and the notes of one vertical.

    notes.py status --size 300        # which chunks of the selection still have no finished notes
    notes.py count                    # notes per vertical
    notes.py show 03-pay              # notes of one vertical, page by page

verticals.md holds one vertical per line: `NAME | question`.
A reader writes notes/START.md for the chunk read.py --offset START --end END:
one note per line, `NAME | what the message says, with its numbers | message_url`,
and as the last line `# done START-END` once the chunk is read to the end.
"""

import json
import re
import sys
from collections import Counter
from pathlib import Path

import fire

import read

NOTES = Path("notes")
VERTICALS = Path("verticals.md")
DONE = re.compile(r"# done (\d+)-(\d+)\s*\Z")


def _verticals():
    names = [line.split("|", 1)[0].strip() for line in VERTICALS.read_text().splitlines() if "|" in line]
    return [name for name in names if name]


def _notes():
    """(file, NAME, note) for every note line."""
    for path in sorted(NOTES.glob("*.md")):
        for line in path.read_text().splitlines():
            parts = [part.strip() for part in line.split("|")]
            if len(parts) >= 3 and parts[0] and not line.startswith("#"):
                yield path.name, parts[0], " | ".join(parts[1:])


class Notes:
    def status(self, size=300):
        """List the chunks without finished notes (no `# done` line) and notes with an unknown vertical."""
        total = len(read.collapse(read.select(json.loads(read.FILTERED.read_text())["messages"])))
        missing = []
        for start in range(0, total, int(size)):
            end = min(start + int(size), total)
            path = NOTES / f"{start}.md"
            lines = path.read_text().splitlines() if path.exists() else []
            if not lines or not DONE.match(lines[-1]) or DONE.match(lines[-1]).groups() != (str(start), str(end)):
                missing.append(f"--offset {start} --end {end}")
        known = set(_verticals())
        unknown = Counter(name for _, name, _ in _notes() if name not in known)
        chunks = -(-total // int(size))
        out = [f"{chunks - len(missing)} of {chunks} chunks done ({total} entries)"]
        out += [f"not done: {chunk}" for chunk in missing]
        out += [f"unknown vertical {name!r}: {n} notes" for name, n in unknown.most_common()]
        return "\n".join(out)

    def count(self):
        """Notes per vertical from verticals.md."""
        counts = Counter(name for _, name, _ in _notes())
        return "\n".join(f"{counts[name]:>6}  {name}" for name in _verticals())

    def show(self, name, offset=0, max_chars=9000):
        """Print the notes of vertical NAME from OFFSET, as many as fit in MAX_CHARS."""
        notes = [f"{note}" for _, vertical, note in _notes() if vertical == name]
        lines, size, position = [], 0, int(offset)
        while position < len(notes):
            line = f"{position + 1}. {notes[position]}"
            if lines and size + len(line) > int(max_chars):
                break
            lines.append(line)
            size += len(line) + 1
            position += 1
        tail = f"-- {int(offset) + 1 if lines else 0}–{position} of {len(notes)}"
        if position < len(notes):
            tail += f"; next page: --offset {position}"
        return "\n".join(lines + [tail])


def main():
    try:
        fire.Fire(Notes, serialize=lambda value: value)
    except (OSError, ValueError, KeyError) as error:
        print(f"notes: {error}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
