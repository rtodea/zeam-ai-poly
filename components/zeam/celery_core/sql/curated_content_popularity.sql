SELECT
    show_content.show_title,
    show_content.show_id,
    count(distinct log.DeviceIdentifier) as viewers,
    count(distinct log.contentViewEventIdentifier) as sessions,
    round(sum(cast(log.playbackDuration as float))/60.0, 1) as duration_minutes
FROM
    prod.log
    INNER JOIN prod.show_content ON log.contentid = show_content.content_id
WHERE
    log.eventtypeid = 1000
    AND log.playbackstart BETWEEN '{start_date}' AND '{end_date}'
    AND NOT EXISTS (SELECT * from public.internal_traffic WHERE public.internal_traffic.ip_address = log.deviceip)
    {dma_filter}
GROUP BY
    show_content.show_title,
    show_content.show_id
ORDER BY
    viewers DESC
LIMIT {limit};
