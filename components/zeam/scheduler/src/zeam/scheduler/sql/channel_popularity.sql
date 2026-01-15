CREATE TEMP TABLE tmp_variables AS SELECT
	'{start_date}'::DATETIME AS start_dates,
  '{end_date}'::DATETIME AS end_dates;

-- Live Summarized with IDs
select
   media_channel.channel_id,
   media_channel.media_id,
   media_channel.dma_id,
   media_channel.friendly_call_sign,
   platform_os.[name] as [Platform],
   [Site].[Name] as [SiteName],
   case
      when [log].eventtypeid = 1119 then 'Zeam'
      when [log].eventtypeid = 1124 then 'Embedded'
      else 'NA'
   end as SiteType,
   case
      when media_channel.allow_out_of_market = 1 then 'Out'
      else 'In'
   end as MarketState,
   dma.dma_name,
   count(distinct [DeviceIdentifier]) as Viewers,
   count(distinct contentViewEventIdentifier) as [Sessions],
   round((sum(cast(playbackDuration as float))/60),1) as [Minutes],
   round((round((sum(cast(playbackDuration as float))/60),1)/count(distinct contentViewEventIdentifier)),1) as [Avg]
from
  prod.[log]
    inner join prod.media_channel on
      [log].ChannelID = media_channel.channel_id
    inner join prod.[Site] on
      [log].SiteID = [Site].site_id
    inner join prod.dma on
      media_channel.dma_id = dma.dma_id
    inner join public.platform_os on
      [log].PlatformOSID = platform_os.platformos_id
where
 	[log].playbackstart between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
  [log].EventTypeID in (1119,1124) and
  [log].playbackDuration > 0 and
  NOT EXISTS (SELECT * from  public.internal_traffic WHERE public.internal_traffic.ip_address = [log].deviceip)
group by
  media_channel.channel_id,
  media_channel.media_id,
  media_channel.dma_id,
  media_channel.friendly_call_sign,
  platform_os.[name],
  [Site].[Name],
  [log].eventtypeid,
  media_channel.allow_out_of_market,
  dma.dma_name
order by
  friendly_call_sign,
  [Platform],
  [SiteName],
  MarketState;
