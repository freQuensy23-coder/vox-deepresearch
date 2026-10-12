"""Bounded-memory validation of Vox JSON/NDJSON responses.

This is a JSON grammar recognizer, not a data loader. It retains only short root
fields used to assess completion, never messages, nested keys, or message text.
"""

import codecs
import json
import math

CHUNK_BYTES = 65536
TOKEN_BYTES = 256
MAX_DEPTH = 128
_STRING_BYTES = bytes(range(32, 256))
_HEX = b"0123456789abcdefABCDEF"
_FIELDS = frozenset(
    {
        "stop_reason",
        "messages",
        "error",
        "errors",
        "ok",
        "success",
        "status",
        "type",
        "partial",
        "is_partial",
        "incomplete",
        "detail",
        "point",
        "progress",
        "processed",
        "processed_messages",
    }
)
_DETAIL_FIELDS = frozenset({"detail", "status", "status_code", "code"})
_PROGRESS_FIELDS = ("point", "progress", "processed", "processed_messages")


class _Container:
    __slots__ = ("kind", "state", "count", "key")

    def __init__(self, kind):
        self.kind = kind
        self.state = "key_or_end" if kind == "object" else "value_or_end"
        self.count = 0
        self.key = None


class CompletionProbe:
    """Validate complete root objects without buffering their message arrays.

    A nesting depth above MAX_DEPTH is unassessed rather than healthy. Otherwise
    malformed JSON, invalid UTF-8 and incomplete terminal frames are failures.
    Multiple root values must be separated by a newline (NDJSON); pretty ordinary
    JSON is also accepted. Each string/number capture is capped at TOKEN_BYTES.
    """

    def __init__(self, search, notify=lambda: None):
        self.search = search
        self.notify = notify
        self.failed = False
        self.final = False
        self.valid = False
        self.unassessed = False
        self.progress_values = {}
        self._utf8 = codecs.getincrementaldecoder("utf-8")("strict")
        self._stack = []
        self._mode = None
        self._token = bytearray()
        self._capture = False
        self._overflow = False
        self._string_key = False
        self._nonempty = False
        self._escape = False
        self._unicode_left = 0
        self._number_state = None
        self._nonzero = False
        self._literal = b""
        self._literal_index = 0
        self._root_values = {}
        self._detail_only = True
        self._documents = 0
        self._between_newline = False
        self._finished = False

    @property
    def buffered_bytes(self):
        """Retained source bytes (excluding bounded stack/field descriptors)."""
        return len(self._token) + len(self._utf8.getstate()[0])

    @property
    def depth(self):
        return len(self._stack)

    def _remember(self, data):
        if self._capture and not self._overflow:
            if len(self._token) + len(data) <= TOKEN_BYTES:
                self._token.extend(data)
            else:
                self._token.clear()
                self._overflow = True

    def _start_string(self, key):
        self._mode = "string"
        self._string_key = key
        self._capture = len(self._stack) == 1 and (
            key or self._stack[-1].key in _FIELDS
        )
        self._token.clear()
        self._overflow = False
        self._nonempty = False
        self._escape = False
        self._unicode_left = 0
        self._remember(b'"')

    def _end_string(self):
        self._remember(b'"')
        value = None
        if self._capture and not self._overflow:
            # At most TOKEN_BYTES; the streaming lexer has already checked escapes.
            value = json.loads(self._token)
        self._token.clear()
        self._mode = None
        if self._string_key:
            parent = self._stack[-1]
            parent.key = value
            parent.state = "colon"
            if len(self._stack) == 1 and value not in _DETAIL_FIELDS:
                self._detail_only = False
        else:
            self._value("string", value, self._nonempty)

    def _string(self, raw, pos):
        """Skip ordinary string runs in C; retain only bounded root captures."""
        size = len(raw)
        quote = raw.find(b'"', pos)
        while pos < size:
            byte = raw[pos]
            if self._unicode_left:
                if byte not in _HEX:
                    self.failed = True
                    return size
                self._remember(raw[pos : pos + 1])
                self._unicode_left -= 1
                pos += 1
            elif self._escape:
                self._escape = False
                self._remember(raw[pos : pos + 1])
                if byte == 117:
                    self._unicode_left = 4
                elif byte not in b'"\\/bfnrt':
                    self.failed = True
                    return size
                pos += 1
            elif byte == 34:
                self._end_string()
                return pos + 1
            elif byte == 92:
                self._nonempty = True
                self._escape = True
                self._remember(b"\\")
                pos += 1
            else:
                if 0 <= quote < pos:
                    quote = raw.find(b'"', pos)
                slash = raw.find(b"\\", pos, quote if quote >= 0 else size)
                end = min(quote if quote >= 0 else size, slash if slash >= 0 else size)
                run = raw[pos:end]
                if run.translate(None, _STRING_BYTES):
                    self.failed = True
                    return size
                self._nonempty |= bool(run)
                self._remember(run)
                pos = end
        return pos

    def _start_number(self, byte):
        self._mode = "number"
        self._capture = len(self._stack) == 1 and self._stack[-1].key in _FIELDS
        self._token.clear()
        self._overflow = False
        self._remember(bytes([byte]))
        self._nonzero = 49 <= byte <= 57
        self._number_state = (
            "sign" if byte == 45 else "zero" if byte == 48 else "integer"
        )

    def _number_byte(self, byte):
        state = self._number_state
        digit = 48 <= byte <= 57
        next_state = None
        if state == "sign":
            next_state = (
                "zero" if byte == 48 else "integer" if 49 <= byte <= 57 else None
            )
        elif state in {"zero", "integer"}:
            if digit and state == "integer":
                next_state = "integer"
            elif byte == 46:
                next_state = "decimal"
            elif byte in (69, 101):
                next_state = "exponent"
            elif digit:
                self.failed = True
                return True
            else:
                return False
        elif state == "decimal":
            next_state = "fraction" if digit else None
        elif state == "fraction":
            if digit:
                next_state = "fraction"
            elif byte in (69, 101):
                next_state = "exponent"
            else:
                return False
        elif state == "exponent":
            next_state = (
                "exponent_sign"
                if byte in (43, 45)
                else "exponent_digits"
                if digit
                else None
            )
        elif state == "exponent_sign":
            next_state = "exponent_digits" if digit else None
        elif state == "exponent_digits":
            if digit:
                next_state = "exponent_digits"
            else:
                return False
        if next_state is None:
            self.failed = True
            return True
        if state not in {"exponent", "exponent_sign", "exponent_digits"} and digit:
            self._nonzero |= byte != 48
        self._number_state = next_state
        self._remember(bytes([byte]))
        return True

    def _end_number(self):
        if self._number_state not in {"zero", "integer", "fraction", "exponent_digits"}:
            self.failed = True
            return
        value = None
        truthy = self._nonzero
        if self._capture and not self._overflow:
            value = float(self._token)
            truthy = value != 0
        self._token.clear()
        self._mode = None
        self._value("number", value, truthy)

    def _value(self, kind, value, truthy):
        if not self._stack:
            self._document(kind)
            return
        parent = self._stack[-1]
        if parent.kind == "object":
            if len(self._stack) == 1 and parent.key in _FIELDS:
                self._root_values[parent.key] = (kind, value, truthy)
            parent.key = None
        parent.count = 1
        parent.state = "comma_or_end"

    def _document(self, kind):
        self._documents = 1
        self._between_newline = False
        self.valid = True
        if kind != "object":
            return
        fields = self._root_values

        def truthy(key):
            return fields.get(key, (None, None, False))[2]

        def scalar(key):
            return fields.get(key, (None, None, False))[1]

        def false_bool(key):
            return fields.get(key, ()) == ("bool", False, False)

        bad = (
            any(
                truthy(k)
                for k in ("error", "errors", "partial", "is_partial", "incomplete")
            )
            or false_bool("ok")
            or false_bool("success")
            or scalar("status") in ("error", "failed")
            or scalar("type") == "error"
            or (truthy("detail") and self._detail_only)
        )
        self.failed |= bad
        if bad:
            return
        if (
            scalar("stop_reason") == "finished"
            and fields.get("messages", (None,))[0] == "array"
        ):
            if not self.final:
                self.notify()
            self.final = True
        for name in _PROGRESS_FIELDS:
            kind, value, _ = fields.get(name, (None, None, False))
            if (
                kind == "number"
                and value is not None
                and math.isfinite(value)
                and value > self.progress_values.get(name, 0)
            ):
                self.progress_values[name] = value
                self.notify()

    def _begin_value(self, byte):
        if not self._stack:
            if self._documents and not self._between_newline:
                self.failed = True
                return
            self._root_values = {}
            self._detail_only = True
        if byte in (123, 91):
            if len(self._stack) >= MAX_DEPTH:
                self.unassessed = True
                return
            self._stack.append(_Container("object" if byte == 123 else "array"))
        elif byte == 34:
            self._start_string(False)
        elif byte == 45 or 48 <= byte <= 57:
            self._start_number(byte)
        elif byte in (116, 102, 110):
            self._mode = "literal"
            self._literal = {116: b"true", 102: b"false", 110: b"null"}[byte]
            self._literal_index = 1
        else:
            self.failed = True

    def _close_container(self):
        frame = self._stack.pop()
        self._value(frame.kind, None, frame.count != 0)

    def _parse(self, raw):
        pos = 0
        while pos < len(raw) and not self.failed and not self.unassessed:
            byte = raw[pos]
            if self._mode == "string":
                pos = self._string(raw, pos)
                continue
            if self._mode == "number":
                if self._number_byte(byte):
                    pos += 1
                    continue
                self._end_number()
                continue
            if self._mode == "literal":
                if byte != self._literal[self._literal_index]:
                    self.failed = True
                    break
                self._literal_index += 1
                if self._literal_index == len(self._literal):
                    literal = self._literal
                    self._mode = None
                    self._value(
                        "null" if literal == b"null" else "bool",
                        literal == b"true" if literal != b"null" else None,
                        literal == b"true",
                    )
                pos += 1
                continue
            if byte in b" \t\r\n":
                if not self._stack and byte == 10:
                    self._between_newline = True
                pos += 1
                continue
            if not self._stack:
                self._begin_value(byte)
                pos += 1
                continue
            parent = self._stack[-1]
            if parent.kind == "object":
                if parent.state in {"key_or_end", "key"}:
                    if byte == 34:
                        self._start_string(True)
                    elif byte == 125 and parent.state == "key_or_end":
                        self._close_container()
                    else:
                        self.failed = True
                elif parent.state == "colon":
                    if byte == 58:
                        parent.state = "value"
                    else:
                        self.failed = True
                elif parent.state == "value":
                    self._begin_value(byte)
                elif byte == 44:
                    parent.state = "key"
                elif byte == 125:
                    self._close_container()
                else:
                    self.failed = True
            elif parent.state in {"value_or_end", "value"}:
                if byte == 93 and parent.state == "value_or_end":
                    self._close_container()
                else:
                    self._begin_value(byte)
            elif byte == 44:
                parent.state = "value"
            elif byte == 93:
                self._close_container()
            else:
                self.failed = True
            pos += 1

    def feed(self, chunk):
        if self._finished:
            raise ValueError("probe already finished")
        for start in range(0, len(chunk), CHUNK_BYTES):
            raw = bytes(chunk[start : start + CHUNK_BYTES])
            try:
                self._utf8.decode(raw, final=False)
            except UnicodeError:
                self.failed = True
            if not self.failed and not self.unassessed:
                self._parse(raw)

    def finish(self):
        if not self._finished:
            self._finished = True
            try:
                self._utf8.decode(b"", final=True)
            except UnicodeError:
                self.failed = True
            if not self.unassessed:
                if self._mode == "number" and not self.failed:
                    self._end_number()
                if self._mode is not None or self._stack:
                    self.failed = True
        if self.failed:
            return "failure"
        if self.unassessed:
            return "neutral"
        if self.search:
            return "success" if self.final else "failure"
        return "success" if self.valid else "neutral"
