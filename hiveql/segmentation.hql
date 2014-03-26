-- this query finds all users that have pictures linked out from photobucket and returns useful dimensions like media count etc
SET hive.exec.parrallel=true;
SET mapred.job.priority=LOW;
SET hive.variable.substitute=true;
SET hive.cli.print.header=true;
SET day = 2014-03-13;

SELECT count(distinct(user_id)) n_users, 
		AVG(pv) AVGpv, 
		AVG(age) AVGAge, 
		count(distinct(case when user_type_id > 0 then user_id end)) n_pro, 
		AVG(media_count) avg_media_count, 
		MIN(media_count) as min_media_count,
		PERCENTILE(media_count, 0.25) as q25_media_count,
		PERCENTILE(media_count, 0.50) as q50_media_count,
		PERCENTILE(media_count, 0.75) as q75_media_count,
		MAX(media_count) as max_media_count,
		AVG(monthly_bytes_transferred)  avg_mbt, 
		MIN(monthly_bytes_transferred) as min_mbt,
		PERCENTILE(monthly_bytes_transferred, 0.25) as q25_mbt,
		PERCENTILE(monthly_bytes_transferred, 0.50) as q50_mbt,
		PERCENTILE(monthly_bytes_transferred, 0.75) as q75_mbt,
		MAX(monthly_bytes_transferred) as max_mmbt
		--AVG(media_bytes_stored)/1048576 avg_mbs,
		--MIN(media_bytes_stored)/1048576 as min_mbs,
		--PERCENTILE(media_bytes_stored, 0.25)/1048576 as q25_mbs,
		--PERCENTILE(media_bytes_stored, 0.50)/1048576 as q50_mbs,
		--PERCENTILE(media_bytes_stored, 0.75)/1048576 as q75_mbs,
		--MAX(media_bytes_stored)/1048576 as max_mbs

FROM
(	
	SELECT umd.user_id, media_count, media_bytes_stored, CAST(monthly_bytes_transferred AS BIGINT) as monthly_bytes_transferred, age, pv, user_type_id
	FROM
	(	
		SELECT user_id
        FROM logging.objlink_daily
        WHERE dt>=DATE_SUB('${hiveconf:day}', 150)
        AND dt <= '${hiveconf:day}'
        AND domain not like 'com.photobucket%'
        GROUP BY user_id
    ) umd

    JOIN
    (
    	SELECT user_id, 
    	datediff('${hiveconf:day}', join_date)/30 as age, 
    	CAST(media_count as BIGINT) as media_count,
    	sqrt(CAST(monthly_bytes_transferred AS BIGINT)) as monthly_bytes_transferred, 
    	CAST(media_bytes_stored AS BIGINT) as media_bytes_stored
        FROM photobucket.user_stats
        WHERE ds = '${hiveconf:day}'

    ) us on umd.user_id = us.user_id

    LEFT OUTER JOIN
    (
    	SELECT user_id, sum(page_views) as pv
        FROM photobucket.album_preference
        WHERE ds ='${hiveconf:day}'
        group by user_id
    ) ap on us.user_id = ap.user_id

    JOIN
    (
    	SELECT user_id, user_type_id
    	FROM photobucket.user_info
    	WHERE ds = '${hiveconf:day}'

    ) ui on ap.user_id = ui.user_id
)u0
