#!/usr/bin/env python3
"""Concurrent research bookkeeping, without search or relevance decisions (POSIX)."""

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import tempfile
import unicodedata


class RegistryError(ValueError):
    """An invalid registry or transition; the original file remains untouched."""


def required(value):
    if not isinstance(value, str) or not value.strip():
        raise RegistryError("Expected nonempty text")
    return value.strip()


def keyword_key(value):
    return " ".join(unicodedata.normalize("NFKC", required(value)).casefold().split())


def group_id(value):
    if isinstance(value, bool) or not re.fullmatch(r"-?\d+", str(value)):
        raise RegistryError("Group ID must be an observed nonzero integer")
    result = int(value)
    if not result:
        raise RegistryError("Group ID must be nonzero")
    return result


def now():
    return datetime.now(timezone.utc).isoformat()


def validate(data):
    if not isinstance(data, dict) or data.get("version") != 1:
        raise RegistryError("Unsupported or corrupt registry")
    for kind in ("groups", "keywords"):
        section = data.get(kind)
        if not isinstance(section, dict) or set(section) != {"todo", "done"}:
            raise RegistryError(f"Invalid {kind} lists")
        seen = set()
        for state in ("todo", "done"):
            records = section[state]
            if not isinstance(records, list):
                raise RegistryError(f"Invalid {kind}.{state}")
            for item in records:
                if not isinstance(item, dict):
                    raise RegistryError("Invalid record")
                if kind == "groups":
                    key = group_id(item.get("id"))
                    if type(item.get("id")) is not int:
                        raise RegistryError("Stored group IDs must be integers")
                else:
                    key = keyword_key(item.get("text"))
                    if item.get("key") != key:
                        raise RegistryError("Invalid keyword key")
                if key in seen:
                    raise RegistryError("Duplicate record across todo/done")
                seen.add(key)
                sources = item.get("sources")
                if not isinstance(sources, list) or not sources:
                    raise RegistryError("Missing provenance")
                for source in sources:
                    if not isinstance(source, dict):
                        raise RegistryError("Invalid provenance")
                    for field in ("source", "evidence", "reason"):
                        required(source.get(field))
                if "assignment" in item:
                    if (
                        kind != "groups"
                        or state != "todo"
                        or not isinstance(item["assignment"], dict)
                    ):
                        raise RegistryError("Invalid assignment")
                    for field in ("owner", "folder", "claimed_at"):
                        required(item["assignment"].get(field))
                if "attempts" in item:
                    if kind != "groups" or not isinstance(item["attempts"], list):
                        raise RegistryError("Invalid attempts")
                    for attempt in item["attempts"]:
                        if not isinstance(attempt, dict) or attempt.get(
                            "status"
                        ) not in ("partial", "error"):
                            raise RegistryError("Invalid attempt")
                        for field in (
                            "owner",
                            "folder",
                            "reason",
                            "claimed_at",
                            "released_at",
                        ):
                            required(attempt.get(field))
                if state == "done":
                    for field in (
                        ("folder", "result", "owner", "completed_at")
                        if kind == "groups"
                        else ("search_evidence", "completed_at")
                    ):
                        required(item.get(field))
                    if kind == "groups" and item.get("assessment") not in (
                        "researched",
                        "excluded",
                    ):
                        raise RegistryError("Invalid group assessment")
    return data


class Registry:
    def __init__(self, path):
        self.path = Path(path).expanduser().absolute()
        # Never lock the JSON inode: os.replace changes it while waiters still hold it.
        self.lock_path = self.path.with_name(self.path.name + ".lock")

    @contextmanager
    def locked(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a", encoding="utf-8") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)

    def read(self):
        try:
            return validate(json.loads(self.path.read_text(encoding="utf-8")))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise RegistryError(f"Cannot read registry: {exc}") from exc

    def write(self, data):
        validate(data)
        fd, name = tempfile.mkstemp(
            prefix=self.path.name + ".", suffix=".tmp", dir=self.path.parent
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(data, stream, ensure_ascii=False, indent=2)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, self.path)
            directory = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if os.path.exists(name):
                os.unlink(name)

    def init(self):
        with self.locked():
            if self.path.exists():
                return self.read()
            data = {
                "version": 1,
                "groups": {"todo": [], "done": []},
                "keywords": {"todo": [], "done": []},
            }
            self.write(data)
            return data

    def snapshot(self):
        with self.locked():
            return self.read()

    def add(self, kind, value, *, source, evidence, reason):
        if kind not in ("groups", "keywords"):
            raise RegistryError("Unknown record kind")
        provenance = {
            "source": required(source),
            "evidence": required(evidence),
            "reason": required(reason),
        }
        key = group_id(value) if kind == "groups" else keyword_key(value)
        key_field = "id" if kind == "groups" else "key"
        with self.locked():
            data = self.read()
            for state in ("todo", "done"):
                for item in data[kind][state]:
                    if item[key_field] == key:
                        if provenance not in item["sources"]:
                            item["sources"].append(provenance)
                            self.write(data)
                        return {"state": state, "record": item, "added": False}
            item = {key_field: key, "sources": [provenance]}
            if kind == "keywords":
                item["text"] = required(value)
            data[kind]["todo"].append(item)
            self.write(data)
            return {"state": "todo", "record": item, "added": True}

    @staticmethod
    def pending(data, kind, key):
        field = "id" if kind == "groups" else "key"
        for item in data[kind]["todo"]:
            if item[field] == key:
                return item
        raise RegistryError("Record is not todo; read the current registry")

    @staticmethod
    def owned(item, owner):
        if item.get("assignment", {}).get("owner") != required(owner):
            raise RegistryError("Group is not claimed by this owner")

    def claim_group(self, value, *, owner, folder):
        owner = required(owner)
        folder = str(Path(required(str(folder))).expanduser().resolve())
        with self.locked():
            data = self.read()
            item = self.pending(data, "groups", group_id(value))
            if "assignment" in item:
                raise RegistryError(
                    "Group is already claimed; do not dispatch it again"
                )
            item["assignment"] = {"owner": owner, "folder": folder, "claimed_at": now()}
            self.write(data)
            return item

    def release_group(self, value, *, owner, reason, status="partial"):
        if status not in ("partial", "error"):
            raise RegistryError("Release status must be partial or error")
        with self.locked():
            data = self.read()
            item = self.pending(data, "groups", group_id(value))
            self.owned(item, owner)
            attempt = dict(
                item["assignment"],
                status=status,
                reason=required(reason),
                released_at=now(),
            )
            item.setdefault("attempts", []).append(attempt)
            del item["assignment"]
            self.write(data)
            return item

    def complete_group(
        self, value, *, owner, result, assessment="researched", remaining_todo=None
    ):
        if assessment not in ("researched", "excluded"):
            raise RegistryError("Assessment must be researched or excluded")
        with self.locked():
            data = self.read()
            item = self.pending(data, "groups", group_id(value))
            self.owned(item, owner)
            folder = Path(item["assignment"]["folder"]).resolve()
            result = Path(result).expanduser().resolve()
            if not result.is_relative_to(folder) or result.suffix.lower() != ".md":
                raise RegistryError(
                    "Result must be a Markdown file inside the assigned folder"
                )
            try:
                text = result.read_text(encoding="utf-8")
            except (OSError, UnicodeError) as exc:
                raise RegistryError(f"Cannot read result: {exc}") from exc
            if not 1 <= len(text.split()) <= 500:
                raise RegistryError(
                    "Facts must be nonempty and at most 500 words (one-page convention)"
                )
            if assessment == "researched" and not re.search(r"https?://\S+", text):
                raise RegistryError("Facts must contain a source URL")
            if remaining_todo is not None:
                pending = Path(remaining_todo).expanduser().resolve()
                if not pending.is_relative_to(folder) or not pending.is_file():
                    raise RegistryError(
                        "Remaining todo must be an existing file inside the assigned folder"
                    )
                item["remaining_todo"] = str(pending)
            item.update(
                folder=str(folder),
                result=str(result),
                owner=owner,
                assessment=assessment,
                completed_at=now(),
            )
            del item["assignment"]
            data["groups"]["todo"].remove(item)
            data["groups"]["done"].append(item)
            self.write(data)
            return item

    def complete_keyword(self, value, *, evidence):
        path = Path(evidence).expanduser().resolve()
        if not path.is_file() or path.stat().st_size == 0:
            raise RegistryError("Save search evidence before completing a keyword")
        if path.suffix.lower() == ".json":
            try:
                saved = json.loads(path.read_text(encoding="utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise RegistryError("Search evidence JSON is invalid") from exc
            if isinstance(saved, dict) and (
                saved.get("status")
                in ("running", "paused", "partial", "error", "unresolved")
                or saved.get("traversal_complete") is False
                or saved.get("stop_reason") == "timeout"
            ):
                raise RegistryError(
                    "Search evidence is incomplete; keep this keyword todo"
                )
        with self.locked():
            data = self.read()
            item = self.pending(data, "keywords", keyword_key(value))
            item.update(search_evidence=str(path), completed_at=now())
            data["keywords"]["todo"].remove(item)
            data["keywords"]["done"].append(item)
            self.write(data)
            return item


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", required=True)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("init", "show"):
        commands.add_parser(name)
    for kind in ("group", "keyword"):
        sub = commands.add_parser("add-" + kind)
        sub.add_argument("value")
        for field in ("source", "evidence", "reason"):
            sub.add_argument("--" + field, required=True)
    for name in ("claim-group", "release-group", "complete-group"):
        sub = commands.add_parser(name)
        sub.add_argument("value")
        sub.add_argument("--owner", required=True)
        if name == "claim-group":
            sub.add_argument("--folder", required=True)
        elif name == "release-group":
            sub.add_argument("--reason", required=True)
            sub.add_argument(
                "--status", choices=("partial", "error"), default="partial"
            )
        else:
            sub.add_argument("--result", required=True)
            sub.add_argument(
                "--assessment", choices=("researched", "excluded"), default="researched"
            )
            sub.add_argument("--remaining-todo")
    sub = commands.add_parser("complete-keyword")
    sub.add_argument("value")
    sub.add_argument("--evidence", required=True)
    args = vars(parser.parse_args(argv))
    registry = Registry(args.pop("registry"))
    command = args.pop("command")
    try:
        if command in ("add-group", "add-keyword"):
            output = registry.add(
                "groups" if command == "add-group" else "keywords", **args
            )
        else:
            method = "snapshot" if command == "show" else command.replace("-", "_")
            output = getattr(registry, method)(**args)
    except (RegistryError, OSError) as exc:
        parser.exit(1, f"registry: {exc}\n")
    print(json.dumps(output, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
