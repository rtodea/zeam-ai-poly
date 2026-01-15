-- Example SQL query for fetching top live streams
-- This is a template - adjust based on your actual schema

SELECT
    stream_id,
    channel_id,
    title,
    viewer_count,
    start_time,
    category
FROM streams
WHERE
    is_live = true
    AND viewer_count > 0
ORDER BY viewer_count DESC
LIMIT 100;
