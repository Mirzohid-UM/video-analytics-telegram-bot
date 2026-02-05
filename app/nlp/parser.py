from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional, Literal

import httpx
import orjson

from app.nlp.prompts import SYSTEM_PROMPT

_JSON_OBJ_RE = re.compile(r"\{.*\}", re.DOTALL)

Entity = Literal["videos", "snapshots"]
Operation = Literal["count", "sum", "distinct_count"]
Comparison = Literal["none", "gt", "lt", "eq", "gte", "lte"]

# ---------- deterministic number parsing (handles "10 000", "10,000", NBSP)
_RE_NUM_ANY = re.compile(r"(\d[\d\s\u00A0_.,]*)")
def _parse_int_human(s: str) -> Optional[int]:
    s = s.replace("\u00A0", " ").strip()
    if not s:
        return None
    s = re.sub(r"[ \t_.,]", "", s)
    return int(s) if s.isdigit() else None

def extract_threshold_ru(text: str) -> Optional[int]:
    t = text.lower().replace("\u00A0", " ")
    for pat in [
        r"(?:больше|более)\s+([0-9][0-9\s\u00A0_.,]*)",
        r">\s*([0-9][0-9\s\u00A0_.,]*)",
        r"(?:не\s*менее)\s+([0-9][0-9\s\u00A0_.,]*)",
        r"(?:минимум)\s+([0-9][0-9\s\u00A0_.,]*)",
    ]:
        m = re.search(pat, t)
        if m:
            return _parse_int_human(m.group(1))
    return None

# ---------- deterministic RU date parsing
RU_MONTHS = {
    "январ": 1, "феврал": 2, "март": 3, "апрел": 4, "ма": 5, "июн": 6,
    "июл": 7, "август": 8, "сентябр": 9, "октябр": 10, "ноябр": 11, "декабр": 12,
}

def _iso(y: int, m: int, d: int) -> str:
    return f"{y:04d}-{m:02d}-{d:02d}"

# "28 ноября 2025"
_RE_DATE1 = re.compile(r"\b(\d{1,2})\s+([а-яё]+)\s+(\d{4})\b", re.IGNORECASE)
# "с 1 по 5 ноября 2025"
_RE_RANGE1 = re.compile(r"\bс\s+(\d{1,2})\s+по\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})\b", re.IGNORECASE)
# "с 1 ноября 2025 по 5 ноября 2025"
_RE_RANGE2 = re.compile(r"\bс\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})\s+по\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})\b", re.IGNORECASE)
# ISO date already
_RE_ISO = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")

def _month_num(word: str) -> Optional[int]:
    w = word.lower()
    for k, v in RU_MONTHS.items():
        if w.startswith(k):
            return v
    return None

def extract_dates_ru(text: str) -> dict[str, str]:
    t = text.lower()

    # range: "с 1 по 5 ноября 2025"
    m = _RE_RANGE1.search(t)
    if m:
        d1, d2, mon, y = int(m.group(1)), int(m.group(2)), m.group(3), int(m.group(4))
        mm = _month_num(mon)
        if mm:
            return {"date_from": _iso(y, mm, d1), "date_to": _iso(y, mm, d2)}

    # range: "с 1 ноября 2025 по 5 ноября 2025"
    m = _RE_RANGE2.search(t)
    if m:
        d1, mon1, y1 = int(m.group(1)), m.group(2), int(m.group(3))
        d2, mon2, y2 = int(m.group(4)), m.group(5), int(m.group(6))
        mm1, mm2 = _month_num(mon1), _month_num(mon2)
        if mm1 and mm2:
            return {"date_from": _iso(y1, mm1, d1), "date_to": _iso(y2, mm2, d2)}

    # single "28 ноября 2025"
    m = _RE_DATE1.search(t)
    if m:
        d, mon, y = int(m.group(1)), m.group(2), int(m.group(3))
        mm = _month_num(mon)
        if mm:
            return {"date": _iso(y, mm, d)}

    # ISO in text
    m = _RE_ISO.search(t)
    if m:
        return {"date": m.group(1)}

    return {}

@dataclass(frozen=True)
class ParseResult:
    entity: Entity
    operation: Operation
    field: str
    comparison: Comparison
    value: int = 0
    creator_id: Optional[str] = None
    date: Optional[str] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None

class LLMParseError(Exception):
    pass

def _extract_json(text: str) -> dict[str, Any]:
    m = _JSON_OBJ_RE.search(text.strip())
    if not m:
        raise LLMParseError("LLM did not return a JSON object")
    raw = m.group(0)
    try:
        return orjson.loads(raw)
    except Exception as e:
        raise LLMParseError(f"Invalid JSON from LLM: {e}") from e

_ALLOWED_ENTITY = {"videos", "snapshots"}
_ALLOWED_OP = {"count", "sum", "distinct_count"}
_ALLOWED_CMP = {"none", "gt", "lt", "eq", "gte", "lte"}

def _validate(obj: dict[str, Any], text: str) -> ParseResult:
    if "error" in obj:
        raise LLMParseError(str(obj["error"]))

    entity = obj.get("entity")
    operation = obj.get("operation")
    field = obj.get("field")
    comparison = obj.get("comparison", "none")
    value = obj.get("value", 0)

    if entity not in _ALLOWED_ENTITY:
        raise LLMParseError("entity missing/invalid")
    if operation not in _ALLOWED_OP:
        raise LLMParseError("operation missing/invalid")
    if comparison not in _ALLOWED_CMP:
        raise LLMParseError("comparison missing/invalid")
    if not isinstance(field, str) or not field.strip():
        raise LLMParseError("field missing/invalid")

    # creator_id normalize
    creator_id = obj.get("creator_id")
    if creator_id is not None:
        if not isinstance(creator_id, (str, int)):
            raise LLMParseError("creator_id must be str or int")
        creator_id = str(creator_id).strip() or None

    # value fallback: if comparison != none and value missing/0 but text has "больше 10 000"
    if not isinstance(value, int):
        # sometimes LLM returns "10000"
        if isinstance(value, str):
            vv = _parse_int_human(value)
            if vv is None:
                raise LLMParseError("value must be int")
            value = vv
        else:
            raise LLMParseError("value must be int")

    if comparison != "none" and value == 0:
        # if it's about "больше N" but value accidentally 0
        thr = extract_threshold_ru(text)
        if thr is not None:
            value = thr

    # deterministic dates fallback
    dates = extract_dates_ru(text)
    date = obj.get("date") or dates.get("date")
    date_from = obj.get("date_from") or dates.get("date_from")
    date_to = obj.get("date_to") or dates.get("date_to")

    # small rule: if text explicitly has ISO range "2025-01-01 до 2025-02-01"
    iso_all = _RE_ISO.findall(text)
    if len(iso_all) >= 2 and (not date_from or not date_to):
        date_from, date_to = iso_all[0], iso_all[1]

    # Basic requiredness (universal)
    # - videos with creator filter allowed, dates for videos allowed (video_created_at)
    # - snapshots date is common; if text says exact day, usually we need date.
    # We don't hard-fail too aggressively; executor will build SQL if enough.
    return ParseResult(
        entity=entity,
        operation=operation,
        field=field.strip(),
        comparison=comparison,
        value=int(value),
        creator_id=creator_id,
        date=date,
        date_from=date_from,
        date_to=date_to,
    )

async def parse_query(ollama_url: str, model: str, text: str) -> ParseResult:
    llm_out = await ollama_chat(ollama_url, model, text)
    obj = _extract_json(llm_out)
    return _validate(obj, text)

async def ollama_chat(
    ollama_url: str,
    model: str,
    user_text: str,
    timeout_sec: float = 60.0,
) -> str:
    url = ollama_url.rstrip("/") + "/api/generate"
    prompt = f"{SYSTEM_PROMPT}\n\nUSER: {user_text}\nASSISTANT:"
    payload = {"model": model, "prompt": prompt, "stream": False, "options": {"temperature": 0}}

    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()
        data = r.json()
        return data["response"]
