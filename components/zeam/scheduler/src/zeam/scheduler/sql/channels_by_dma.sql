select
  dma.dma_name,
  dma.dma_id,
  count(*) channelCount
from
  prod.media_channel
    inner join prod.site_channel ON
      media_channel.channel_id = site_channel.channel_id and
      site_channel.site_id = 91
    inner join prod.dma on
      media_channel.dma_id = dma.dma_id
group by
  dma.dma_name,
  dma.dma_id
order by
  channelCount desc;
