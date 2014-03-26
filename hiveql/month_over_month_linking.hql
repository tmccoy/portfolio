-- this query finds the number of users month over month that have media linked externally from photobucket
SET hive.variable.substitute = true;
SET mapred.job.priority=LOW;
SET day=2014-03-14;


SELECT mth , yr, COUNT(DISTINCT(user_id)) as users

FROM
(
        SELECT user_id, month(dt) as mth, year(dt) as yr
        FROM logging.objlink_daily
        WHERE dt>=DATE_SUB('${hiveconf:day}', 150)
        AND dt <= '${hiveconf:day}'
        AND domain not like 'com.photobucket%'
        GROUP BY user_id, month(dt), year(dt)

) umd 

GROUP BY mth, yr 
