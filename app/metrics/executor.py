from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.db import DB
from app.nlp.parser import ParseResult

UTC = timezone.utc

def _to_utc_day_bounds(date_iso: str) -> tuple[datetime, datetime]:
    dt = datetime.fromisoformat(date_iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end

def _to_utc_period_bounds(date_from: str, date_to: str) -> tuple[datetime, datetime]:
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
    end = d2.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)  # inclusive
    return start, end

# Whitelists
VIDEO_FIELDS = {
    "views": "views_count",
    "likes": "likes_count",
    "comments": "comments_count",
    "reports": "reports_count",
    "video_id": "id",
}
SNAPSHOT_FIELDS = {
    "views": "views_count",
    "likes": "likes_count",
    "comments": "comments_count",
    "reports": "reports_count",
    "delta_views": "delta_views_count",
    "delta_likes": "delta_likes_count",
    "delta_comments": "delta_comments_count",
    "delta_reports": "delta_reports_count",
    "video_id": "video_id",
}

CMP_OP = {
    "gt": ">",
    "lt": "<",
    "eq": "=",
    "gte": ">=",
    "lte": "<=",
}

async def execute_metric(db: DB, pr: ParseResult) -> int:
    # choose table + columns
    if pr.entity == "videos":
        table = "videos"
        field_map = VIDEO_FIELDS
        time_col = "video_created_at"
    else:
        table = "video_snapshots"
        field_map = SNAPSHOT_FIELDS
        time_col = "created_at"

    col = field_map.get(pr.field)
    if not col:
        raise ValueError(f"Unsupported field: {pr.field}")

    # select expression
    if pr.operation == "count":
        select = "COUNT(*)::bigint"
    elif pr.operation == "distinct_count":
        if pr.field != "video_id":
            # distinct makes sense mostly for video_id
            raise ValueError("distinct_count requires field=video_id")
        select = f"COUNT(DISTINCT {col})::bigint"
    elif pr.operation == "sum":
        # sum only numeric cols (especially delta_*)
        select = f"COALESCE(SUM(COALESCE({col},0)),0)::bigint"
    else:
        raise ValueError(f"Unsupported operation: {pr.operation}")

    where = []
    params: list[Any] = []

    # filters: creator_id only for videos
    if pr.creator_id is not None:
        if pr.entity != "videos":
            # snapshots don't have creator_id directly (unless you join; keep it simple)
            raise ValueError("creator_id filter only supported for videos")
        where.append("creator_id = %s")
        params.append(pr.creator_id)

    # date / period filters
    if pr.date:
        start, end = _to_utc_day_bounds(pr.date)
        where.append(f"{time_col} >= %s")
        where.append(f"{time_col} <  %s")
        params.extend([start, end])
    elif pr.date_from and pr.date_to:
        start, end = _to_utc_period_bounds(pr.date_from, pr.date_to)
        where.append(f"{time_col} >= %s")
        where.append(f"{time_col} <  %s")
        params.extend([start, end])

    # comparison on the chosen field
    if pr.comparison != "none":
        op = CMP_OP.get(pr.comparison)
        if not op:
            raise ValueError(f"Unsupported comparison: {pr.comparison}")
        where.append(f"COALESCE({col},0) {op} %s")
        params.append(int(pr.value))

    sql = f"SELECT {select} FROM {table}"
    if where:
        sql += " WHERE " + " AND ".join(where)

    val = await db.fetchval(sql, tuple(params))
    return int(val or 0)
