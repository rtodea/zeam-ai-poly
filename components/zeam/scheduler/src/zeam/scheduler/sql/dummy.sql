-- This is a placeholder query for fetching popular content
-- In a real implementation, this would aggregate view logs from Redshift
SELECT 
    'channel' as content_type,
    '123' as content_id,
    'Popular Channel' as title,
    4001 as dma_id
UNION ALL
SELECT 
    'show' as content_type,
    '456' as content_id,
    'Popular Show' as title,
    4001 as dma_id
UNION ALL
SELECT 
    'vod' as content_type,
    '789' as content_id,
    'Popular Movie' as title,
    null as dma_id -- Global popularity

