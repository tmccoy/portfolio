-- this query finds all users that have organized photos and returns some demographic information
SET hive.variable.substitute=true;
SET hive.cli.print.header=true;
SET mapred.job.priority=LOW;
SET day=2013-03-14;

SELECT COUNT(DISTINCT(ui.user_id)) as total_users,
       COUNT(DISTINCT(CASE WHEN sex = 'F' THEN ui.user_id END)) as female,
       COUNT(DISTINCT(CASE WHEN sex = 'M' THEN ui.user_id END)) as male, 
       COUNT(DISTINCT(CASE WHEN DATEDIFF('${hiveconf:day}', birth_date)/365 BETWEEN 13 AND 17 THEN ui.user_id END )) as 13_17, 
       COUNT(DISTINCT(CASE WHEN DATEDIFF('${hiveconf:day}', birth_date)/365 BETWEEN 18 AND 24 THEN ui.user_id END )) as 18_24, 
       COUNT(DISTINCT(CASE WHEN DATEDIFF('${hiveconf:day}', birth_date)/365 BETWEEN 25 AND 34 THEN ui.user_id END )) as 25_34,                                                                                                                             
       COUNT(DISTINCT(CASE WHEN DATEDIFF('${hiveconf:day}', birth_date)/365 BETWEEN 35 AND 44 THEN ui.user_id END )) as 35_44,                                                                                                                             
       COUNT(DISTINCT(CASE WHEN DATEDIFF('${hiveconf:day}', birth_date)/365 BETWEEN 45 AND 54 THEN ui.user_id END )) as 45_54,                                                                                                                             
       COUNT(DISTINCT(CASE WHEN DATEDIFF('${hiveconf:day}', birth_date)/365 > 55 THEN ui.user_id END )) as 55_,
       COUNT(DISTINCT(CASE WHEN valid_email = 1 THEN up.user_id END )) as users_with_valid_email,
       COUNT(DISTINCT(CASE WHEN email_opt_out = 1 THEN up.user_id END )) as users_opted_out


FROM
(
      SELECT user_id
      FROM tracking.user_metric_daily
      WHERE ds >= date_sub('${hiveconf:day}', 150)
      AND ds <= '${hiveconf:day}'
      AND metric_reference_id IN (21658,367842,832325,367864,367857)
      GROUP BY user_id
) umd0

JOIN
(
      SELECT user_id
      FROM logging.user_daily
      WHERE dt>=DATE_SUB('${hiveconf:day}', 150)
      AND dt <= '${hiveconf:day}'
      GROUP BY user_id

) umd on umd0.user_id = umd.user_id  

LEFT OUTER JOIN
(

        SELECT user_id, sex, birth_date
        FROM photobucket.user_info
        WHERE ds = '${hiveconf:day}'

) ui on umd.user_id = ui.user_id

LEFT OUTER JOIN
(

        SELECT user_id, valid_email, email_opt_out
        FROM photobucket.user_preference
        WHERE ds = '${hiveconf:day}'

) up on ui.user_id = up.user_id
