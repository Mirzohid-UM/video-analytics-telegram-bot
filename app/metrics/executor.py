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

# ... yuqoridagi whitelist/UTC funksiyalar o'zgarishsiz

async def execute_metric(db: DB, pr: ParseResult) -> int:
    if pr.entity == "videos":
        base_from = "FROM videos"
        field_map = VIDEO_FIELDS
        time_col = "video_created_at"
        creator_filter_sql = "creator_id = %s"
    else:
        # snapshots
        # creator_id bo'lsa JOIN qilamiz
        base_from = "FROM video_snapshots s"
        field_map = SNAPSHOT_FIELDS
        time_col = "s.created_at"
        creator_filter_sql = "v.creator_id = %s"

    col = field_map.get(pr.field)
    if not col:
        raise ValueError(f"Unsupported field: {pr.field}")

    # snapshots uchun col prefiksi
    if pr.entity == "snapshots":
        if col in ("video_id",):
            col_expr = f"s.{col}"
        else:
            col_expr = f"s.{col}"
    else:
        col_expr = col  # videos already

    # select
    if pr.operation == "count":
        select = "COUNT(*)::bigint"
    elif pr.operation == "distinct_count":
        if pr.field != "video_id":
            raise ValueError("distinct_count requires field=video_id")
        select = f"COUNT(DISTINCT {col_expr})::bigint"
    elif pr.operation == "sum":
        select = f"COALESCE(SUM(COALESCE({col_expr},0)),0)::bigint"
    else:
        raise ValueError(f"Unsupported operation: {pr.operation}")

    where = []
    params = []

    # creator filter
    join = ""
    if pr.creator_id is not None:
        if pr.entity == "snapshots":
            join = " JOIN videos v ON v.id = s.video_id"
        where.append(creator_filter_sql)
        params.append(pr.creator_id)

    # date filters
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

    # comparison
    if pr.comparison != "none":
        op = CMP_OP.get(pr.comparison)
        if not op:
            raise ValueError(f"Unsupported comparison: {pr.comparison}")
        where.append(f"COALESCE({col_expr},0) {op} %s")
        params.append(int(pr.value))

    sql = f"SELECT {select} {base_from}{join}"
    if where:
        sql += " WHERE " + " AND ".join(where)

    val = await db.fetchval(sql, tuple(params))
    return int(val or 0)
