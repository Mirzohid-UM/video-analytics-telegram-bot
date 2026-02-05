from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.db import DB
from app.metrics.queries import SQL
from app.nlp.parser import ParseResult

UTC = timezone.utc

def _day_bounds_iso(date_iso: str) -> tuple[datetime, datetime]:
    start = datetime.fromisoformat(date_iso).replace(tzinfo=UTC)
    end = start + timedelta(days=1)
    return start, end

def _period_bounds_iso(date_from: str, date_to: str) -> tuple[datetime, datetime]:
    start = datetime.fromisoformat(date_from).replace(tzinfo=UTC)
    end = datetime.fromisoformat(date_to).replace(tzinfo=UTC) + timedelta(days=1)  # inclusive
    return start, end

async def execute_metric(db: DB, pr: ParseResult) -> int:
    metric = pr.metric
    sql = SQL[metric]

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

    return 0
