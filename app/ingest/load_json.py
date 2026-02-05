from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import orjson

from app.config import load_settings
from app.db import DB

UTC = timezone.utc


def _ts(v: str) -> datetime:
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    dt = datetime.fromisoformat(v)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


async def main(json_path: str) -> None:
    settings = load_settings()
    db = DB(settings.database_url)
    await db.connect()

    raw = Path(json_path).read_bytes()
    data = orjson.loads(raw)

    videos = data["videos"] if isinstance(data, dict) else data

    video_rows: list[tuple] = []
    snap_rows: list[tuple] = []

    for v in videos:
        vid = str(v["id"])
        creator_id = str(v["creator_id"])

        video_rows.append(
            (
                vid,
                creator_id,
                _ts(v["video_created_at"]),
                int(v.get("views_count", 0)),
                int(v.get("likes_count", 0)),
                int(v.get("comments_count", 0)),
                int(v.get("reports_count", 0)),
            )
        )

        for s in v.get("snapshots", []):
            snap_rows.append(
                (
                    str(s["id"]),
                    str(s.get("video_id") or vid),  # fallback
                    int(s.get("views_count", 0)),
                    int(s.get("likes_count", 0)),
                    int(s.get("comments_count", 0)),
                    int(s.get("reports_count", 0)),
                    int(s.get("delta_views_count", 0)),
                    int(s.get("delta_likes_count", 0)),
                    int(s.get("delta_comments_count", 0)),
                    int(s.get("delta_reports_count", 0)),
                    _ts(s["created_at"]),
                )
            )

    await db.executemany(
        """
        INSERT INTO videos (
          id, creator_id, video_created_at,
          views_count, likes_count, comments_count, reports_count
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
          creator_id=EXCLUDED.creator_id,
          video_created_at=EXCLUDED.video_created_at,
          views_count=EXCLUDED.views_count,
          likes_count=EXCLUDED.likes_count,
          comments_count=EXCLUDED.comments_count,
          reports_count=EXCLUDED.reports_count,
          updated_at=NOW()
        """,
        video_rows,
    )

    await db.executemany(
        """
        INSERT INTO video_snapshots (
          id, video_id,
          views_count, likes_count, comments_count, reports_count,
          delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
          created_at
        )
        VALUES  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
          video_id=EXCLUDED.video_id,
          views_count=EXCLUDED.views_count,
          likes_count=EXCLUDED.likes_count,
          comments_count=EXCLUDED.comments_count,
          reports_count=EXCLUDED.reports_count,
          delta_views_count=EXCLUDED.delta_views_count,
          delta_likes_count=EXCLUDED.delta_likes_count,
          delta_comments_count=EXCLUDED.delta_comments_count,
          delta_reports_count=EXCLUDED.delta_reports_count,
          created_at=EXCLUDED.created_at,
          updated_at=NOW()
        """,
        snap_rows,
    )

    await db.close()
    print(f"Loaded videos={len(video_rows)} snapshots={len(snap_rows)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m app.ingest.load_json /path/to/file.json")
        raise SystemExit(2)

    import asyncio

    asyncio.run(main(sys.argv[1]))
