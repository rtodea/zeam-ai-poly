CREATE TEMP TABLE tmp_variables AS SELECT
	'{start_date}'::DATETIME AS start_dates,
  '{end_date}'::DATETIME AS end_dates;

-- Live Summarized
select
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
  -- [log].SiteID in (91) and
  [log].EventTypeID in (1119,1124) and
  [log].playbackDuration > 0 and
  NOT EXISTS (SELECT * from  public.internal_traffic WHERE public.internal_traffic.ip_address = [log].deviceip)
group by
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

-- VOD Summarized
select
  media_channel.Friendly_call_sign,
  case
    when media_channel.allow_out_of_market = 1 then 'Out'
    else 'In'
  end as MarketState,
  dma.dma_name,
  case
    when log.EventTypeID = 1121 then 'VOD'
    else 'Clip'
  end as MediaType,
  platform_os.[name] as platformName,
  count(distinct [log].DeviceIdentifier) as Viewers,
  count(distinct [log].contentViewEventIdentifier) as [Sessions],
  round(sum(([log].playbackDuration/60.0)),1) as [Duration],
  adRequests.requests,
  adRequests.fills,
  case sum(adRequests.requests) when 0 then 0 else round((cast(sum(adRequests.fills) as float)/cast(sum(adRequests.requests) as float)) * 100,0) end as "%"
from
  [prod].[log]
    inner join public.platform_os on
      log.PlatformOSID = platform_os.platformos_id
    inner join prod.media_channel on
      log.ChannelID = media_channel.channel_id
    inner join prod.dma on
      media_channel.dma_id = dma.dma_id
    left outer join
    (
      select
        innerlog.platformosid,
        parentLog.channelid,
        parentLog.eventtypeid,
        sum(case when innerLog.eventtypeid = 108 then 1 else 0 end) as requests,
        sum(case when innerLog.eventtypeid = 101 then 1 else 0 end) as fills
      from
        [prod].[log] innerlog
          inner join prod.vod ON
            innerlog.mediaID = vod.media_id
          inner join prod.log parentLog ON
            innerlog.contentvieweventidentifier = parentLog.contentvieweventidentifier and
            parentlog.eventtypeid in (1116, 1121)
          inner join prod.media_channel ON
            parentLog.channelid = media_channel.channel_id
      where
        [innerlog].eventtime between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
        NOT EXISTS (SELECT * from  public.internal_traffic WHERE public.internal_traffic.ip_address = [innerlog].deviceip) and
        innerlog.EventTypeID in (101, 108)
      group by
        innerlog.platformosid,
        parentLog.channelid,
        parentLog.eventtypeid
    ) as adRequests on
      log.platformosid = adRequests.platformosid and
      log.channelid = adRequests.channelid and
      log.eventtypeid = adRequests.eventtypeid
where
  [log].playbackstart between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
  NOT EXISTS (SELECT * from  public.internal_traffic WHERE public.internal_traffic.ip_address = [log].deviceip) and
  [log].SiteID = 91 and
  [log].EventTypeID in (1116, 1121)
group by
  MarketState,
  dma.dma_name,
  media_channel.Friendly_call_sign,
  platform_os.[name],
  MediaType,
  adRequests.requests,
  adRequests.fills
order by
	Friendly_call_sign,
  MediaType,
  platformName;

-- Summary by channel by program
select
  media_channel.friendly_call_sign,
  case
    when media_channel.allow_out_of_market = 1 then 'Out'
    else 'In'
  end as MarketState,
  Program.Title as Title,
  platform_os.[name] as [Platform],
  count(distinct log.contentViewEventIdentifier) as [Sessions],
  round((sum(cast(log.playbackDuration as float))/60),1) as [Minutes],
  round((round((sum(cast(log.playbackDuration as float))/60),1)/count(distinct log.contentViewEventIdentifier)),1) as [Avg]
from
  [prod].[log]
    inner join prod.media_channel on
      [log].ChannelID = media_channel.channel_id
    inner join public.platform_os on
      [log].PlatformOSID = platform_os.platformos_id
    inner join prod.program on
      [log].ProgramID = program.program_id and
      program.program_id != -1
where
  [log].playbackstart between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
  NOT EXISTS (SELECT * from  public.internal_traffic WHERE public.internal_traffic.ip_address = [log].deviceip) and
  [log].SiteID = 91 and
  [log].EventTypeID = 1119 and
  [log].playbackDuration > 0
group by
  media_channel.friendly_call_sign,
  media_channel.allow_out_of_market,
  media_channel.media_id,
  program.title,
  platform_os.[name]
order by
  media_channel.friendly_call_sign,
  media_channel.allow_out_of_market,
  program.title,
  platform_os.[name];

-- Curated Summarized
select
	show_content.show_title,
  show_content.licensor_name,
  platform_os.[name] as platformName,
  count(distinct [log].DeviceIdentifier) as Viewers,
  count(distinct [log].contentViewEventIdentifier) as [Sessions],
  count(distinct [log].contentid) as [Episodes],
  round(sum(([log].playbackDuration/60.0)),1) as [Duration],
  adRequests.requests,
  adRequests.fills,
  case adRequests.requests when 0 then 0 else round((cast(adRequests.fills as float)/cast(adRequests.requests as float)) * 100,0) end as "%"
from
  [prod].[log]
    inner join public.platform_os on
      log.PlatformOSID = platform_os.platformos_id
    inner join prod.show_content on
      log.contentid = show_content.content_id
    left outer join
    (
      select
        innerlog.platformosid,
        show_content.show_id,
        parentLog.eventtypeid,
        sum(case when innerLog.eventtypeid = 1008 then 1 else 0 end) as requests,
        sum(case when innerLog.eventtypeid = 1001 then 1 else 0 end) as fills
      from
        [prod].[log] innerlog
          inner join prod.log parentLog ON
            innerlog.contentvieweventidentifier = parentLog.contentvieweventidentifier and
            parentlog.eventtypeid in (1000)
          inner join prod.show_content on
            innerlog.contentid = show_content.content_id
      where
        [innerlog].eventtime between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
        NOT EXISTS (SELECT * from  public.internal_traffic WHERE public.internal_traffic.ip_address = [innerlog].deviceip) and
        innerlog.EventTypeID in (1001, 1008)
      group by
        innerlog.platformosid,
        show_content.show_id,
        parentLog.eventtypeid
    ) as adRequests on
      log.platformosid = adRequests.platformosid and
      show_content.show_id = adRequests.show_id and
      log.eventtypeid = adRequests.eventtypeid
where
  [log].playbackstart between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
  NOT EXISTS (SELECT * from  public.internal_traffic WHERE public.internal_traffic.ip_address = [log].deviceip) and
  [log].SiteID = 91 and
  [log].EventTypeID in (1000)
group by
	show_title,
  show_content.licensor_name,
  platformName,
  adRequests.requests,
  adRequests.fills
order by
	show_title,
  platformName;

-- Curated Summarized by Show by Episode
select
	show_content.show_title,
	show_content.content_title,
  platform_os.[name] as platformName,
  count(distinct [log].DeviceIdentifier) as Viewers,
  count(distinct [log].contentViewEventIdentifier) as [Sessions],
  round(sum(([log].playbackDuration/60.0)),1) as [Duration],
  adRequests.requests,
  adRequests.fills,
  case adRequests.requests when 0 then 0 else round((cast(adRequests.fills as float)/cast(adRequests.requests as float)) * 100,0) end as "%"
from
  [prod].[log]
    inner join public.platform_os on
      log.PlatformOSID = platform_os.platformos_id
    inner join prod.show_content on
      log.contentid = show_content.content_id
    left outer join
    (
      select
        innerlog.platformosid,
        show_content.show_id,
        show_content.content_id,
        parentLog.eventtypeid,
        sum(case when innerLog.eventtypeid = 1008 then 1 else 0 end) as requests,
        sum(case when innerLog.eventtypeid = 1001 then 1 else 0 end) as fills
      from
        [prod].[log] innerlog
          inner join prod.log parentLog ON
            innerlog.contentvieweventidentifier = parentLog.contentvieweventidentifier and
            parentlog.eventtypeid in (1000)
          inner join prod.show_content on
            innerlog.contentid = show_content.content_id
      where
        [innerlog].eventtime between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
        NOT EXISTS (SELECT * from  public.internal_traffic WHERE public.internal_traffic.ip_address = [innerlog].deviceip) and
        innerlog.EventTypeID in (1001, 1008)
      group by
        innerlog.platformosid,
        show_content.show_id,
        show_content.content_id,
        parentLog.eventtypeid
    ) as adRequests on
      log.platformosid = adRequests.platformosid and
      show_content.show_id = adRequests.show_id and
      show_content.content_id = adRequests.content_id and
      log.eventtypeid = adRequests.eventtypeid
where
  [log].playbackstart between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
  NOT EXISTS (SELECT * from  public.internal_traffic WHERE public.internal_traffic.ip_address = [log].deviceip) and
  [log].SiteID = 91 and
  [log].EventTypeID in (1000)
group by
	show_title,
	show_content.content_title,
  platformName,
  adRequests.requests,
  adRequests.fills
order by
	show_title,
	show_content.content_title,
  platformName;

--Combined V4
with sessionStats as
(
      select
        ad_sessions.site_id,
        ad_sessions.channel_id,
        ad_sessions.platform_os_id,
        count(distinct ad_sessions.session_id) as sessions,
        SUM(ad_session_stats.live_duration) / 60000 AS live_duration_minutes,
        SUM(ad_session_stats.ad_duration) / 60000 AS ad_duration_minutes,
        SUM(ad_session_stats.adpad_duration) / 60000 AS adpad_duration_minutes,
        SUM(ad_session_stats.live_duration + ad_session_stats.ad_duration + ad_session_stats.adpad_duration + ad_session_stats.offair_duration
          + ad_session_stats.tech_duration + ad_session_stats.unk_duration) / 60000 AS total_duration_minutes
      from
        public.ad_session_stats
          inner join public.ad_sessions ON
            ad_session_stats.session_id = ad_sessions.session_id
      WHERE
        updated_ts between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
        ad_sessions.environment like ('prod%') and
        ((ad_session_stats.session_id not like 'samsung%') and
        (ad_session_stats.session_id not like 'amazon%') and
        (ad_session_stats.session_id not like 'localnow%') and
        (ad_session_stats.session_id not like 'roku%') and
        (ad_session_stats.session_id not like 'fubo%'))
      group BY
        ad_sessions.site_id,
        ad_sessions.channel_id,
        ad_sessions.platform_os_id
),
adStats AS
(
  SELECT
    ad_sessions.site_id,
    ad_sessions.channel_id,
    ad_sessions.platform_os_id,
    SUM(CASE WHEN ad_fills.vast_ad_id IS NOT NULL THEN 1 ELSE 0 END) as "Filled",
    SUM(CASE WHEN ((ad_fills.vast_ad_id IS NULL) and (ad_fills.session_id is not null)) THEN 1 ELSE 0 END) as "Not Filled",
    SUM(CASE WHEN ad_fills.ad_fill_id <> 9 THEN 1 ELSE 0 END) as "Total"
  FROM
    ad_sessions
      left outer JOIN ad_fills ON
        ad_sessions.session_id = ad_fills.session_id
  WHERE
    ad_fills.created_ts between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
    ad_sessions.environment like ('prod%') and
    ad_fills.session_id is not null
  GROUP BY
    ad_sessions.site_id,
    ad_sessions.channel_id,
    ad_sessions.platform_os_id
)
select
  sessionStats.site_id,
  site.[Name],
  media_channel.friendly_call_sign,
  platform_os.name as "Platform",
  adStats."Filled",
  adStats."Not Filled",
  adStats."Total",
  sessionStats.sessions,
  sessionStats.live_duration_minutes,
  sessionStats.ad_duration_minutes,
  sessionStats.adpad_duration_minutes,
  sessionStats.total_duration_minutes
from
  sessionStats
    left OUTER JOIN adStats on
      sessionStats.site_id = adStats.site_id and
      sessionStats.channel_id = adStats.channel_id and
      sessionStats.platform_os_id = adStats.platform_os_id
    INNER join prod.site ON
      sessionStats.site_id = site.site_id
    INNER join prod.media_channel ON
      sessionStats.channel_id = media_channel.channel_id
    INNER JOIN public.platform_os ON
      sessionStats.platform_os_id = platform_os.platformos_id
ORDER BY
  sessionStats.site_id,
	media_channel.friendly_call_sign,
  platform_os.name;


--  	-- All V4 ads
-- 	SELECT
-- 		ad_sessions.site_id,
--     site.[Name],
-- 		media_channel.friendly_call_sign,
-- 		ad_sessions.channel_id,
--     public.platform_os.name as "Platform",
-- 		SUM(CASE WHEN ad_fills.vast_ad_id IS NOT NULL THEN 1 ELSE 0 END) as "Filled",
-- 		SUM(CASE WHEN ad_fills.vast_ad_id IS NULL THEN 1 ELSE 0 END) as "Not Filled",
-- 		SUM(CASE WHEN ad_fills.ad_fill_id <> 9 THEN 1 ELSE 0 END) as "Total",
-- 		ROUND(CAST(SUM(CASE WHEN ad_fills.vast_ad_id IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) /
-- 		    CAST(SUM(CASE WHEN ad_fills.ad_fill_id <> 9 THEN 1 ELSE 0 END) AS FLOAT) * 100, 2) as "Fill Rate"
-- 	FROM
-- 		ad_fills
-- 			INNER JOIN ad_sessions
-- 				ON ad_fills.session_id = ad_sessions.session_id
-- 			INNER JOIN prod.media_channel   -- To get the friendly call sign.
-- 				ON ad_sessions.Channel_ID = media_channel.Channel_ID
-- 			INNER JOIN public.platform_os
-- 				ON ad_sessions.platform_os_id = platform_os.platformos_id
--       INNER join prod.site ON
--         ad_sessions.site_id = site.site_id
-- 	WHERE
--     ad_fills.created_ts between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
-- 		ad_sessions.environment like ('prod%') and
--     ad_sessions.site_id not in (806, 812, 866, 1187, 1311, 790)
-- 	GROUP BY
-- 		ad_sessions.site_id,
--     site.[Name],
-- 		media_channel.friendly_call_sign,
--     public.platform_os.name,
-- 		ad_sessions.channel_id
-- 	ORDER BY
-- 		ad_sessions.site_id,
-- 		media_channel.friendly_call_sign,
--     public.platform_os.name,
-- 		ad_sessions.channel_id;

-- -- VUit ads
-- 	SELECT
-- 		ad_sessions.site_id,
--     site.[Name],
-- 		media_channel.friendly_call_sign,
-- 		ad_sessions.channel_id,
--     public.platform_os.name as "Platform",
-- 		SUM(CASE WHEN ad_fills.vast_ad_id IS NOT NULL THEN 1 ELSE 0 END) as "Filled",
-- 		SUM(CASE WHEN ad_fills.vast_ad_id IS NULL THEN 1 ELSE 0 END) as "Not Filled",
-- 		SUM(CASE WHEN ad_fills.ad_fill_id <> 9 THEN 1 ELSE 0 END) as "Total",
-- 		ROUND(CAST(SUM(CASE WHEN ad_fills.vast_ad_id IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT) /
-- 		    CAST(SUM(CASE WHEN ad_fills.ad_fill_id <> 9 THEN 1 ELSE 0 END) AS FLOAT) * 100, 2) as "Fill Rate"
-- 	FROM
-- 		ad_fills
-- 			INNER JOIN ad_sessions
-- 				ON ad_fills.session_id = ad_sessions.session_id
-- 			INNER JOIN prod.media_channel   -- To get the friendly call sign.
-- 				ON ad_sessions.Channel_ID = media_channel.Channel_ID
-- 			INNER JOIN public.platform_os
-- 				ON ad_sessions.platform_os_id = platform_os.platformos_id
--       INNER join prod.site ON
--         ad_sessions.site_id = site.site_id
-- 	WHERE
--     ad_fills.created_ts between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
-- 		ad_sessions.environment like ('prod%') AND
-- 		ad_sessions.site_id in (91)
-- 	GROUP BY
-- 		ad_sessions.site_id,
--     site.[Name],
-- 		media_channel.friendly_call_sign,
--     public.platform_os.name,
-- 		ad_sessions.channel_id
-- 	ORDER BY
-- 		ad_sessions.site_id,
-- 		media_channel.friendly_call_sign,
--     public.platform_os.name,
-- 		ad_sessions.channel_id;

-- -- Gray Ads
-- SELECT
-- 	ad_sessions.site_id,
--   site.[Name],
-- 	media_channel.friendly_call_sign,
-- 	ad_sessions.channel_id,
--   public.platform_os.name as "Platform",
-- 	SUM(CASE WHEN ad_fills.vast_ad_id IS NOT NULL THEN 1 ELSE 0 END) as "Filled",
-- 	SUM(CASE WHEN ad_fills.vast_ad_id IS NULL THEN 1 ELSE 0 END) as "Not Filled",
-- 	SUM(CASE WHEN ad_fills.ad_fill_id <> 9 THEN 1 ELSE 0 END) as "Total",
-- 	ROUND(CAST(SUM(CASE WHEN ad_fills.vast_ad_id IS NOT NULL THEN 1 ELSE 0 END) AS FLOAT)/
--     CAST(SUM(CASE WHEN ad_fills.ad_fill_id <> 9 THEN 1 ELSE 0 END) AS FLOAT) * 100, 2) as "Fill Rate"
-- FROM
-- 	ad_fills
-- 		INNER JOIN ad_sessions ON
-- 			ad_fills.session_id = ad_sessions.session_id and
--       ad_sessions.site_id != 91
--     INNER JOIN public.platform_os ON
--       ad_sessions.platform_os_id = platform_os.platformos_id
-- 		inner JOIN prod.media_channel ON
-- 			ad_sessions.Channel_ID = media_channel.Channel_ID and
--       ((media_channel.station_operator_id = 1393) or
--       (media_channel.station_owner_id = 1393))
-- 		INNER join prod.site ON
-- 			ad_sessions.site_id = site.site_id
-- WHERE
--   ad_fills.created_ts between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
-- 	ad_sessions.environment like ('prod%')
-- GROUP BY
-- 	ad_sessions.site_id,
--   site.[Name],
-- 	media_channel.friendly_call_sign,
--   public.platform_os.name,
-- 	ad_sessions.channel_id
-- ORDER BY
-- 	ad_sessions.site_id,
-- 	media_channel.friendly_call_sign,
--   public.platform_os.name,
-- 	ad_sessions.channel_id;

-- -- Viewer Metrics
-- SELECT
--   ad_sessions.site_id,
--   site.[Name],
--   media_channel.friendly_call_sign,
--     public.platform_os.name as "Platform",
--   COUNT(ad_session_stats.*) AS session_count,
--   SUM(ad_session_stats.live_duration) / 60000 AS live_duration_minutes,
--   SUM(ad_session_stats.ad_duration) / 60000 AS ad_duration_minutes,
--   SUM(ad_session_stats.adpad_duration) / 60000 AS adpad_duration_minutes,
--   SUM(ad_session_stats.tech_duration) / 60000 AS tech_duration_minutes,
--   SUM(ad_session_stats.offair_duration) / 60000 AS offair_minutes,
--   SUM(ad_session_stats.unk_duration) / 60000 AS unk_minutes,
--   SUM(ad_session_stats.live_duration + ad_session_stats.ad_duration + ad_session_stats.adpad_duration + ad_session_stats.offair_duration
--     + ad_session_stats.tech_duration + ad_session_stats.unk_duration) / 60000 AS total_duration_minutes,
--   SUM(ad_session_stats.live_duration) / 60000 + SUM(ad_session_stats.ad_duration) / 60000 + SUM(ad_session_stats.adpad_duration) / 60000
--     + SUM(ad_session_stats.tech_duration) / 60000 +  SUM(ad_session_stats.offair_duration) / 60000 + SUM(ad_session_stats.unk_duration) / 60000 AS total2
-- FROM
-- 	ad_session_stats
-- 		INNER join public.ad_sessions ON
-- 			ad_sessions.session_id = ad_session_stats.session_id
-- 		INNER join prod.site ON
-- 			ad_sessions.site_id = site.site_id
-- 		INNER join prod.media_channel ON
-- 			ad_sessions.channel_id = media_channel.channel_id
--     INNER JOIN public.platform_os
--       ON ad_sessions.platform_os_id = platform_os.platformos_id
-- WHERE
--   ad_session_stats.updated_ts between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
-- 	ad_sessions.environment like ('prod%') and
-- 	((ad_session_stats.session_id not like 'samsung%') and
-- 	(ad_session_stats.session_id not like 'fubo%'))
-- GROUP BY
-- 	ad_sessions.site_id,
-- 	site.[Name],
-- 	media_channel.friendly_call_sign,
--   public.platform_os.name
-- ORDER BY
-- 	ad_sessions.site_id,
-- 	site.[Name],
-- 	media_channel.friendly_call_sign,
--   public.platform_os.name;

  -- Minutes Viewed Summary
select
   platform_os.[name] as [Platform],
   count(distinct [log].[DeviceIdentifier]) as Viewers,
   count(distinct [log].contentViewEventIdentifier) as [Sessions],
   round((sum(cast([log].playbackDuration as float))/60),1) as [Minutes],
   round(((sum(cast([log].playbackDuration as float))/60)/60),1) as [Hours]
from
   [prod].[log]
      inner join public.platform_os on
         [log].PlatformOSID = platform_os.platformos_id
where
   [log].playbackstart between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
   [log].SiteID = 91 and
   [log].EventTypeID = 1119 and
   [log].playbackDuration > 0 and
   NOT EXISTS (SELECT * from  public.internal_traffic WHERE public.internal_traffic.ip_address = [log].deviceip)
group by
   platform_os.[name];
-- OPTION (MAXDOP 2)



-- -- Total VOD
select
   platform_os.[name] as [Platform],
   count(distinct [log].[DeviceIdentifier]) as Viewers,
   count(distinct [log].contentViewEventIdentifier) as [Sessions],
   round((sum(cast([log].playbackDuration as float))/60),1) as [Minutes],
   round(((sum(cast([log].playbackDuration as float))/60)/60),1) as [Hours]
from
   [prod].[log]
      inner join public.platform_os on
         [log].PlatformOSID = platform_os.platformos_id
where
   [log].playbackstart between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
   [log].SiteID = 91 and
   [log].EventTypeID = 1121 and
   --[log].playbackDuration >= 30 and
   NOT EXISTS (SELECT * from  public.internal_traffic WHERE public.internal_traffic.ip_address = [log].deviceip)
group by
   platform_os.[name];
-- OPTION (MAXDOP 2)


--Total Event
select
   platform_os.[name] as [Platform],
   count(distinct [log].[DeviceIdentifier]) as Viewers,
   count(distinct [log].contentViewEventIdentifier) as [Sessions],
   round((sum(cast([log].playbackDuration as float))/60),1) as [Minutes],
   round(((sum(cast([log].playbackDuration as float))/60)/60),1) as [Hours]
from
  [prod].[log]
    inner join public.platform_os on
      [log].PlatformOSID = platform_os.platformos_id
where
   [log].playbackstart between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
   [log].SiteID = 91 and
   [log].EventTypeID = 1126 and
   --[log].playbackDuration >= 30 and
   NOT EXISTS (SELECT * from  public.internal_traffic WHERE public.internal_traffic.ip_address = [log].deviceip)
group by
   platform_os.[name];
-- OPTION (MAXDOP 2)

--Total Curated
select
   platform_os.[name] as [Platform],
   count(distinct [log].[DeviceIdentifier]) as Viewers,
   count(distinct [log].contentViewEventIdentifier) as [Sessions],
   round((sum(cast([log].playbackDuration as float))/60),1) as [Minutes],
   round(((sum(cast([log].playbackDuration as float))/60)/60),1) as [Hours]
from
  [prod].[log]
      inner join public.platform_os on
         [log].PlatformOSID = platform_os.platformos_id
where
   [log].playbackstart between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
   [log].SiteID = 91 and
   [log].EventTypeID = 1000 and
   --[log].playbackDuration >= 30 and
   NOT EXISTS (SELECT * from  public.internal_traffic WHERE public.internal_traffic.ip_address = [log].deviceip)
group by
   platform_os.[name];
-- OPTION (MAXDOP 2)

--Total All
select
   platform_os.[name] as [Platform],
   count(distinct [log].[DeviceIdentifier]) as Viewers,
   count(distinct [log].contentViewEventIdentifier) as [Sessions],
   round((sum(cast([log].playbackDuration as float))/60),1) as [Minutes],
   round(((sum(cast([log].playbackDuration as float))/60)/60),1) as [Hours]
from
  [prod].[log]
      inner join public.platform_os on
         [log].PlatformOSID = platform_os.platformos_id
where
   [log].playbackstart between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
   [log].SiteID = 91 and
   --[log].playbackDuration >= 30 and
   NOT EXISTS (SELECT * from  public.internal_traffic WHERE public.internal_traffic.ip_address = [log].deviceip)
group by
   platform_os.[name];
-- OPTION (MAXDOP 2)

--Daily DMA
select
  platform_os.[Name],
  dma.dma_name,
	count(distinct [DeviceIdentifier]) as Viewers,
	count(distinct contentViewEventIdentifier) as [Sessions],
	round((sum(cast(playbackDuration as float))/60),1) as [Minutes]
from
  prod.[log]
    inner join public.platform_os on
      [log].PlatformOSID = platform_os.platformos_id
    inner join prod.dma on
        log.dmaid = dma.dma_id
where
  [log].playbackstart between (select start_dates from tmp_variables) and (select end_dates from tmp_variables) and
  NOT EXISTS (SELECT * from  public.internal_traffic WHERE public.internal_traffic.ip_address = [log].deviceip) and
  [log].playbackDuration > 0 and
  [log].SiteID = 91 and
  [log].EventTypeID = 1119
group by
  platform_os.[Name],
  dma.dma_name
order by
  platform_os.[Name];
