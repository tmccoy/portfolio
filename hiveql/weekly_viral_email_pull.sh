#!/bin/sh
# this script pulls emails weekly for a marketing email
today=$(date +%e)

if [ $today -lt 10 ]; # need to add leading 0 to date if it's a single digit 
  then
    today="0""$today"
fi

month=$(date +%m)
year=$(date +%Y)
spaces_date=$year-$month-$today 
date=$(echo "$spaces_date" | sed -e 's/ //g') # strip spaces caused by month/day being single digit 

hiveql="

SET hive.variable.substitute = true;
SET mapred.job.priority=LOW;
SET day=${date};

SELECT DISTINCT(email)
FROM
(
        SELECT user_id
        FROM tracking.user_metric_daily
        WHERE ds >= date_sub('\${hiveconf:day}', 360)
        AND metric_reference_id IN (8,28,194,186566,202741, 1415835)
        GROUP BY user_id
        UNION ALL
        SELECT user_id
        FROM photobucket.api_consumer_user
        WHERE modification_dt >= date_sub('\${hiveconf:day}', 360)
        AND ds = '$\{hiveconf:day}'
        AND scid <> 149831069
        GROUP BY user_id
        UNION ALL
        SELECT user_id
        FROM photobucket.user_stats
        WHERE ds = '\${hiveconf:day}'
        AND TO_DATE(last_accessed_dt) >= date_sub('\${hiveconf:day}', 360)

) umd

JOIN photobucket.global_user g on umd.user_id = g.user_id
JOIN photobucket.user_preference up on g.user_id = up.user_id
JOIN photobucket.user_info ui on up.user_id =ui.user_id
WHERE g.ds = '\${hiveconf:day}'
AND up.ds = '\${hiveconf:day}'
AND ui.ds = '\${hiveconf:day}'
AND up.valid_email = 1
AND up.email_opt_out = 0
AND g.active = 1
;
"

hive -e "$hiveql" > /tmp/raw_viral_$date.csv

awk '{$1=$1}1' /tmp/raw_viral_$date.csv | sed 's/\"//g' > /tmp/viral_$date.csv # remove invalid characters that cause load issues

cp /tmp/viral_$date.csv /home/ldap/misc/email_pulls

rm /tmp/raw_viral_$date.csv
rm /tmp/viral_$date.csv

