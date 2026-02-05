from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional, Union

import httpx
import orjson

from app.nlp.prompts import SYSTEM_PROMPT

_RE_NUM = re.compile(r"(\d[\d\s\u00A0_.,]*)")

_JSON_OBJ_RE = re.compile(r"\{.*\}", re.DOTALL)

ALLOWED_METRICS = {
    "count_videos_total",
    "count_videos_by_creator_period",
    "count_videos_over_views_all_time",
    "count_videos_by_creator_over_views_all_time",
    "sum_delta_views_on_date",
    "count_videos_with_new_views_on_date",
}

@dataclass(frozen=True)
class ParseResult:
    metric: str
    creator_id: Optional[Union[str, int]] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    date: Optional[str] = None
    threshold: Optional[int] = None

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

def _validate(obj: dict[str, Any], text: str) -> ParseResult:
    if "error" in obj:
        raise LLMParseError(str(obj["error"]))

    metric = obj.get("metric")
    if metric not in ALLOWED_METRICS:
        raise LLMParseError("Unknown or missing metric")

    creator_id = obj.get("creator_id")
    if creator_id is not None:
        if not isinstance(creator_id, (str, int)):
            raise LLMParseError("creator_id must be str or int")
        creator_id = str(creator_id).strip()
        if not creator_id:
            raise LLMParseError("creator_id empty")

    threshold = obj.get("threshold")

    # ✅ NEW: fallback extract from RU text (handles "10 000")
    if threshold is None:
        threshold = extract_threshold_ru(text)

    if threshold is not None and not isinstance(threshold, int):
        raise LLMParseError("threshold must be int")

    date_from = obj.get("date_from")
    date_to = obj.get("date_to")
    date = obj.get("date")

    if metric == "count_videos_by_creator_period":
        if creator_id is None or not date_from or not date_to:
            raise LLMParseError("creator_id/date_from/date_to required")

    if metric == "count_videos_over_views_all_time":
        if threshold is None:
            raise LLMParseError("threshold required")

    if metric == "count_videos_by_creator_over_views_all_time":
        if creator_id is None or threshold is None:
            raise LLMParseError("creator_id/threshold required")

    if metric in ("sum_delta_views_on_date", "count_videos_with_new_views_on_date"):
        if not date:
            raise LLMParseError("date required")

    return ParseResult(
        metric=metric,
        creator_id=creator_id,
        date_from=date_from,
        date_to=date_to,
        date=date,
        threshold=threshold,
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
    # Ollama 0.15.x: /api/generate
    url = ollama_url.rstrip("/") + "/api/generate"

    prompt = f"{SYSTEM_PROMPT}\n\nUSER: {user_text}\nASSISTANT:"

    payload = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0},
    }

    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        r = await client.post(url, json=payload)
        r.raise_for_status()
        data = r.json()
        return data["response"]



def _parse_int_human(s: str) -> int | None:
    s = s.replace("\u00A0", " ")
    s = s.strip()
    if not s:
        return None
    s = re.sub(r"[ \t_.,]", "", s)
    return int(s) if s.isdigit() else None

def extract_threshold_ru(text: str) -> int | None:
    t = text.lower().replace("\u00A0", " ")
    for pat in [
        r"(?:больше|более)\s+([0-9][0-9\s\u00A0_.,]*)",
        r">\s*([0-9][0-9\s\u00A0_.,]*)",
    ]:
        m = re.search(pat, t)
        if m:
            return _parse_int_human(m.group(1))
    m = _RE_NUM.search(t)
    return _parse_int_human(m.group(1)) if m else None
