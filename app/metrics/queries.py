SQL = {
    "count_videos_total": """
        SELECT COUNT(*)::bigint
        FROM videos
    """,

    "count_videos_by_creator_period": """
        SELECT COUNT(*)::bigint
        FROM videos
        WHERE creator_id = %s
          AND video_created_at >= %s
          AND video_created_at <  %s
    """,

    "count_videos_over_views_all_time": """
        SELECT COUNT(*)::bigint
        FROM videos
        WHERE views_count > %s
    """,

    "sum_delta_views_on_date": """
        SELECT COALESCE(SUM(delta_views_count), 0)::bigint
        FROM video_snapshots
        WHERE created_at >= %s
          AND created_at <  %s
    """,

    "count_videos_with_new_views_on_date": """
        SELECT COUNT(DISTINCT video_id)::bigint
        FROM video_snapshots
        WHERE created_at >= %s
          AND created_at <  %s
          AND delta_views_count > 0
    """,

    "count_videos_by_creator_over_views_all_time": """
     SELECT COUNT(*)::bigint
     FROM videos
     WHERE creator_id = %s
       AND views_count > %s
""",

}
