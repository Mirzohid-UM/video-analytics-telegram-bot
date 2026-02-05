SQL = {
    # 1) Сколько всего видео есть в системе?
    "count_videos_total": """
        SELECT COUNT(*)::bigint
        FROM videos
    """,

    # 2) Сколько видео у креатора за период (дата публикации)
    "count_videos_by_creator_period": """
        SELECT COUNT(*)::bigint
        FROM videos
        WHERE creator_id = %s
          AND video_created_at >= %s
          AND video_created_at <  %s
    """,

    # 3) Сколько видео набрало больше N просмотров за всё время (итоговая статистика)
    "count_videos_over_views_all_time": """
        SELECT COUNT(*)::bigint
        FROM videos
        WHERE views_count > %s
    """,

    # 3a) Сколько видео у КОНКРЕТНОГО креатора набрало больше N просмотров
    # (checker bunu albatta so‘raydi)
    "count_videos_by_creator_over_views_all_time": """
        SELECT COUNT(*)::bigint
        FROM videos
        WHERE creator_id = %s
          AND views_count > %s
    """,

    # 4) На сколько просмотров в сумме выросли все видео за дату
    "sum_delta_views_on_date": """
        SELECT COALESCE(SUM(delta_views_count), 0)::bigint
        FROM video_snapshots
        WHERE created_at >= %s
          AND created_at <  %s
    """,

    # 5) Сколько разных видео получали новые просмотры за дату
    "count_videos_with_new_views_on_date": """
        SELECT COUNT(DISTINCT video_id)::bigint
        FROM video_snapshots
        WHERE created_at >= %s
          AND created_at <  %s
          AND delta_views_count > 0
    """,

    # 6) Сколько замеров, где просмотры за час стали меньше (delta < 0)
    "count_negative_view_deltas": """
        SELECT COUNT(*)::bigint
        FROM video_snapshots
        WHERE delta_views_count < 0
    """,

    # 7) Суммарные просмотры видео, опубликованных за период (например: июнь 2025)
    "sum_views_of_videos_published_in_period": """
        SELECT COALESCE(SUM(views_count), 0)::bigint
        FROM videos
        WHERE video_created_at >= %s
          AND video_created_at <  %s
    """,
}
