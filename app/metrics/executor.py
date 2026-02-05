from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from app.db import DB
from app.nlp.parser import ParseResult

UTC = timezone.utc

def _dt_utc_day_bounds(date_iso: str) -> tuple[datetime, datetime]:
    dt = datetime.fromisoformat(date_iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    else:
        dt = dt.astimezone(UTC)
    start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)
    return start, end

def _dt_utc_period_bounds(date_from: str, date_to: str) -> tuple[datetime, datetime]:
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

VIDEO_FIELDS = {
    "views": "views_count",
    "likes": "likes_count",
    "comments": "comments_count",
    "reports": "reports_count",
    "video_id": "id",
}
SNAP_FIELDS = {
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

CMP_OP = {"gt": ">", "lt": "<", "eq": "=", "gte": ">=", "lte": "<="}

async def execute_metric(db: DB, pr: ParseResult) -> int:
    params: list[Any] = []
    where: list[str] = []

    if pr.entity == "videos":
        base_from = "FROM videos v"
        join = ""
        field_map = VIDEO_FIELDS
        time_col = "v.video_created_at"
        col = field_map.get(pr.field)
        if not col:
            return 0
        col_expr = f"v.{col}"

        # creator filter
        if pr.creator_id:
            where.append("v.creator_id = %s")
            params.append(pr.creator_id)

    else:
        base_from = "FROM video_snapshots s"
        field_map = SNAP_FIELDS
        col = field_map.get(pr.field)
        if not col:
            return 0
        col_expr = f"s.{col}"
        time_col = "s.created_at"

        join = ""
        if pr.creator_id:
            # support creator filter for snapshots via join
            join = " JOIN videos v ON v.id = s.video_id"
            where.append("v.creator_id = %s")
            params.append(pr.creator_id)

    # time filters
    if pr.date:
        start, end = _dt_utc_day_bounds(pr.date)
        where.append(f"{time_col} >= %s")
        where.append(f"{time_col} <  %s")
        params.extend([start, end])
    elif pr.date_from and pr.date_to:
        start, end = _dt_utc_period_bounds(pr.date_from, pr.date_to)
        where.append(f"{time_col} >= %s")
        where.append(f"{time_col} <  %s")
        params.extend([start, end])

    # comparison
    if pr.comparison != "none":
        op = CMP_OP.get(pr.comparison)
        if not op:
            return 0
        where.append(f"COALESCE({col_expr},0) {op} %s")
        params.append(int(pr.value))

    # select
    if pr.operation == "count":
        select = "COUNT(*)::bigint"
    elif pr.operation == "distinct_count":
        if pr.field != "video_id":
            return 0
        select = f"COUNT(DISTINCT {col_expr})::bigint"
    elif pr.operation == "sum":
        select = f"COALESCE(SUM(COALESCE({col_expr},0)),0)::bigint"
    else:
        return 0

    sql = f"SELECT {select} {base_from}{join}"
    if where:
        sql += " WHERE " + " AND ".join(where)

    val = await db.fetchval(sql, tuple(params))
    return int(val or 0)

