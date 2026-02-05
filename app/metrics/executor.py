from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.db import DB
from app.metrics.queries import SQL
from app.nlp.parser import ParseResult

UTC = timezone.utc

def _day_bounds_iso(date_iso: str) -> tuple[datetime, datetime]:
    dt = datetime.fromisoformat(date_iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)

    start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end


def _period_bounds_iso(date_from: str, date_to: str) -> tuple[datetime, datetime]:
    d1 = datetime.fromisoformat(date_from)
    d2 = datetime.fromisoformat(date_to)

    if d1.tzinfo is None:
        d1 = d1.replace(tzinfo=UTC)
    else:
        d1 = d1.astimezone(UTC)

    if d2.tzinfo is None:
        d2 = d2.replace(tzinfo=UTC)
    else:
        d2 = d2.astimezone(UTC)

    start = d1.replace(hour=0, minute=0, second=0, microsecond=0)
    end = d2.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    return start, end

async def execute_metric(db: DB, pr: ParseResult) -> int:
    metric = pr.metric
    sql = SQL.get(metric)
    if not sql:
        raise ValueError(f"Unknown metric: {metric}")

    if metric == "count_videos_total":
        val = await db.fetchval(sql)
        return int(val or 0)

    if metric == "count_videos_by_creator_period":
        start, end = _period_bounds_iso(pr.date_from, pr.date_to)  # type: ignore[arg-type]
        val = await db.fetchval(sql, (pr.creator_id, start, end))
        return int(val or 0)

    if metric == "count_videos_over_views_all_time":
        val = await db.fetchval(sql, (pr.threshold,))
        return int(val or 0)

    if metric in ("sum_delta_views_on_date", "count_videos_with_new_views_on_date"):
        start, end = _day_bounds_iso(pr.date)  # type: ignore[arg-type]
        val = await db.fetchval(sql, (start, end))
        return int(val or 0)

    raise ValueError(f"Unhandled metric: {metric}")
