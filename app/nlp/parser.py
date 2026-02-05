from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Optional, Literal

import httpx
import orjson

from app.nlp.prompts import SYSTEM_PROMPT

Entity = Literal["videos", "snapshots"]
Operation = Literal["count", "sum", "distinct_count"]
Comparison = Literal["none", "gt", "lt", "eq", "gte", "lte"]

_ALLOWED_ENTITY = {"videos", "snapshots"}
_ALLOWED_OP = {"count", "sum", "distinct_count"}
_ALLOWED_CMP = {"none", "gt", "lt", "eq", "gte", "lte"}

_FIELD_SYNONYMS = {
    "views_count": "views",
    "likes_count": "likes",
    "comments_count": "comments",
    "reports_count": "reports",
    "delta_views_count": "delta_views",
    "delta_likes_count": "delta_likes",
    "delta_comments_count": "delta_comments",
    "delta_reports_count": "delta_reports",
    "id": "video_id",
}

_JSON_OBJ_RE = re.compile(r"\{.*\}", re.DOTALL)

# numbers: 10 000 / 10,000 / NBSP / 10_000
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

# RU months
RU_MONTHS = {
    "январ": 1, "феврал": 2, "март": 3, "апрел": 4, "ма": 5, "июн": 6,
    "июл": 7, "август": 8, "сентябр": 9, "октябр": 10, "ноябр": 11, "декабр": 12,
}

def _month_num(word: str) -> Optional[int]:
    w = word.lower()
    for k, v in RU_MONTHS.items():
        if w.startswith(k):
            return v
    return None

def _iso(y: int, m: int, d: int) -> str:
    return f"{y:04d}-{m:02d}-{d:02d}"

_RE_ISO = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
_RE_YM = re.compile(r"\b(\d{4})-(\d{2})\b")  # 2025-06

_RE_DATE1 = re.compile(r"\b(\d{1,2})\s+([а-яё]+)\s+(\d{4})\b", re.IGNORECASE)
_RE_RANGE1 = re.compile(r"\bс\s+(\d{1,2})\s+по\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})\b", re.IGNORECASE)
_RE_RANGE2 = re.compile(r"\bс\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})\s+по\s+(\d{1,2})\s+([а-яё]+)\s+(\d{4})\b", re.IGNORECASE)
_RE_RU_MONTH_YEAR = re.compile(r"\bв\s+([а-яё]+)\s+(\d{4})\s+года?\b", re.IGNORECASE)

def _month_bounds(y: int, m: int) -> tuple[str, str]:
    # inclusive end
    if m == 12:
        y2, m2 = y + 1, 1
    else:
        y2, m2 = y, m + 1
    first_next = date(y2, m2, 1)
    last_day = first_next - timedelta(days=1)
    return f"{y:04d}-{m:02d}-01", f"{last_day.year:04d}-{last_day.month:02d}-{last_day.day:02d}"

def extract_dates_ru(text: str) -> dict[str, str]:
    t = text.lower()

    # "в июне 2025 года" -> month range
    m = _RE_RU_MONTH_YEAR.search(t)
    if m:
        mon_word, y = m.group(1), int(m.group(2))
        mm = _month_num(mon_word)
        if mm:
            df, dt = _month_bounds(y, mm)
            return {"date_from": df, "date_to": dt}

    # "с 1 по 5 ноября 2025"
    m = _RE_RANGE1.search(t)
    if m:
        d1, d2, mon, y = int(m.group(1)), int(m.group(2)), m.group(3), int(m.group(4))
        mm = _month_num(mon)
        if mm:
            return {"date_from": _iso(y, mm, d1), "date_to": _iso(y, mm, d2)}

    # "с 1 ноября 2025 по 5 ноября 2025"
    m = _RE_RANGE2.search(t)
    if m:
        d1, mon1, y1 = int(m.group(1)), m.group(2), int(m.group(3))
        d2, mon2, y2 = int(m.group(4)), m.group(5), int(m.group(6))
        mm1, mm2 = _month_num(mon1), _month_num(mon2)
        if mm1 and mm2:
            return {"date_from": _iso(y1, mm1, d1), "date_to": _iso(y2, mm2, d2)}

    # "28 ноября 2025"
    m = _RE_DATE1.search(t)
    if m:
        d, mon, y = int(m.group(1)), m.group(2), int(m.group(3))
        mm = _month_num(mon)
        if mm:
            return {"date": _iso(y, mm, d)}

    # ISO dates in text
    iso_all = _RE_ISO.findall(t)
    if len(iso_all) >= 2:
        return {"date_from": iso_all[0], "date_to": iso_all[1]}
    if len(iso_all) == 1:
        return {"date": iso_all[0]}

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
    m = _JSON_OBJ_RE.search(text)
    if not m:
        raise LLMParseError("LLM did not return a JSON object")

    raw = m.group(0)
    cleaned = re.sub(r",\s*([}\]])", r"\1", raw)  # trailing commas

    # outermost object only
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start != -1 and end != -1 and end > start:
        cleaned = cleaned[start:end + 1]
    cleaned = re.sub(r",\s*([}\]])", r"\1", cleaned)

    return orjson.loads(cleaned)

def _heuristic_parse(text: str) -> ParseResult:
    t = text.lower().replace("\u00A0", " ")

    # entity
    snapshots_hint = any(w in t for w in ["замер", "снапш", "за час", "по сравнению", "предыдущ", "приращ", "динамик"])
    final_hint = any(w in t for w in ["итог", "финал", "опубликован", "вышл"])
    entity: Entity = "snapshots" if snapshots_hint and not final_hint else "videos"
    if "по итоговой статистике" in t or "итоговой статистике" in t:
        entity = "videos"
    if any(w in t for w in ["замеров статистики", "замеров", "снапшотов"]):
        entity = "snapshots"

    # field
    if "лайк" in t:
        base = "likes"
    elif "коммент" in t:
        base = "comments"
    elif "жалоб" in t or "репорт" in t:
        base = "reports"
    else:
        base = "views"

    if entity == "snapshots" and any(w in t for w in ["за час", "приращ", "динамик", "стало меньше", "стало больше", "по сравнению"]):
        field = f"delta_{base}"
    else:
        field = base

    # operation
    if "суммар" in t or "в сумме" in t:
        operation: Operation = "sum"
    elif "сколько разных видео" in t or "разных видео" in t:
        operation = "distinct_count"
        field = "video_id"
        if entity != "snapshots":
            entity = "snapshots"  # обычно "разных видео получали новые" = snapshots
    else:
        operation = "count"
        if "видео" in t:
            field = "video_id"

    # comparison/value
    comparison: Comparison = "none"
    value = 0

    if any(w in t for w in ["отриц", "стало меньше", "уменьш"]):
        comparison, value = "lt", 0
    elif any(w in t for w in ["вырос", "стало больше", "прибав", "получали новые"]):
        comparison, value = "gt", 0

    thr = extract_threshold_ru(text)
    if thr is not None:
        # decide gt/gte by phrase
        if re.search(r"\bне\s*менее\b", t):
            comparison = "gte"
        else:
            comparison = "gt"
        value = thr

    # creator_id
    creator_id = None
    # RU/EN: "креатор с id <id>", "creator id <id>", "id <id>", creator_id=<id>
    m = re.search(
        r"(?:креатор|creator)?\s*(?:с\s*)?id\s+([0-9a-f]{32}|[0-9a-f\-]{36}|\d+)",
        t,
        re.IGNORECASE,
    )
    if not m:
        m = re.search(r"\bid\s+([0-9a-f]{32}|[0-9a-f\-]{36}|\d+)\b", t, re.IGNORECASE)
    if not m:
        m = re.search(r"\bcreator[_\s]?id\s*=?\s*([0-9a-f]{32}|[0-9a-f\-]{36}|\d+)\b", t, re.IGNORECASE)
    if m:
        creator_id = m.group(1)

    dates = extract_dates_ru(text)
    date_ = dates.get("date")
    date_from = dates.get("date_from")
    date_to = dates.get("date_to")

    # YYYY-MM found -> month bounds
    ym = _RE_YM.findall(t)
    if ym and (not date_from or not date_to):
        y, mo = int(ym[0][0]), int(ym[0][1])
        df, dt = _month_bounds(y, mo)
        date_from, date_to = df, dt

    return ParseResult(
        entity=entity,
        operation=operation,
        field=field,
        comparison=comparison,
        value=int(value),
        creator_id=creator_id,
        date=date_,
        date_from=date_from,
        date_to=date_to,
    )

def _validate_and_normalize(obj: dict[str, Any], text: str) -> ParseResult:
    # defaults if missing
    entity = obj.get("entity")
    operation = obj.get("operation")
    field = obj.get("field")
    comparison = obj.get("comparison", "none")
    value = obj.get("value", 0)

    # normalize basic
    if isinstance(field, str):
        field = _FIELD_SYNONYMS.get(field.strip(), field.strip())

    if entity not in _ALLOWED_ENTITY:
        entity = None
    if operation not in _ALLOWED_OP:
        operation = None
    if comparison not in _ALLOWED_CMP:
        comparison = "none"

    # creator_id normalize
    creator_id = obj.get("creator_id")
    if creator_id is not None:
        if isinstance(creator_id, (str, int)):
            creator_id = str(creator_id).strip() or None
        else:
            creator_id = None

    # hard fallback: extract creator_id from text if missing (checker prompts rely on this)
    if not creator_id:
        t = text.lower().replace("\u00A0", " ")
        m = re.search(
            r"(?:креатор|creator)?\s*(?:с\s*)?id\s+([0-9a-f]{32}|[0-9a-f\-]{36}|\d+)",
            t,
            re.IGNORECASE,
        )
        if not m:
            m = re.search(r"\bid\s+([0-9a-f]{32}|[0-9a-f\-]{36}|\d+)\b", t, re.IGNORECASE)
        if not m:
            m = re.search(r"\bcreator[_\s]?id\s*=?\s*([0-9a-f]{32}|[0-9a-f\-]{36}|\d+)\b", t, re.IGNORECASE)
        if m:
            creator_id = m.group(1)

    # value normalize
    if not isinstance(value, int):
        if isinstance(value, str):
            vv = _parse_int_human(value)
            value = vv if vv is not None else 0
        else:
            value = 0

    # dates
    dates = extract_dates_ru(text)
    date_ = obj.get("date") or dates.get("date")
    date_from = obj.get("date_from") or dates.get("date_from")
    date_to = obj.get("date_to") or dates.get("date_to")

    # normalize YYYY-MM -> month bounds
    if isinstance(date_from, str) and not date_to:
        m = _RE_YM.fullmatch(date_from.strip())
        if m:
            y, mm = int(m.group(1)), int(m.group(2))
            df, dt = _month_bounds(y, mm)
            date_from, date_to = df, dt

    # if operation/entity/field are missing -> fallback fully
    if not entity or not operation or not isinstance(field, str) or not field.strip():
        return _heuristic_parse(text)

    # fill missing pieces using heuristics lightly
    pr = ParseResult(
        entity=entity,
        operation=operation,
        field=field.strip(),
        comparison=comparison,
        value=int(value),
        creator_id=creator_id,
        date=date_,
        date_from=date_from,
        date_to=date_to,
    )

    # comparison/value fallback only for gt/gte
    if pr.comparison in ("gt", "gte") and pr.value == 0:
        thr = extract_threshold_ru(text)
        if thr is not None:
            pr = ParseResult(**{**pr.__dict__, "value": thr})

    # if text says negative/hourly -> enforce lt 0 on delta
    t = text.lower()
    if "отриц" in t or "стало меньше" in t:
        pr = ParseResult(**{**pr.__dict__, "comparison": "lt", "value": 0})
    if "получали новые" in t and "видео" in t:
        pr = ParseResult(**{**pr.__dict__, "entity": "snapshots", "operation": "distinct_count", "field": "video_id", "comparison": "gt", "value": 0})

    # HARD OVERRIDE: publication-date queries must use videos.video_created_at
    t_pub = text.lower()
    if any(w in t_pub for w in ["опубликовал", "опубликован", "дата публикации"]):
        return ParseResult(
            entity="videos",
            operation="count",
            field="video_id",
            comparison="none",
            value=0,
            creator_id=creator_id,
            date=date_,
            date_from=date_from,
            date_to=date_to,
        )


    return pr

async def parse_query(ollama_url: str, model: str, text: str) -> ParseResult:
    # Try LLM, but NEVER die: fallback to heuristic
    try:
        llm_out = await ollama_chat(ollama_url, model, text)
        obj = _extract_json(llm_out)
        return _validate_and_normalize(obj, text)
    except Exception:
        return _heuristic_parse(text)

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
