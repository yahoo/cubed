SELECT state,logged_in,
       funnel_merge(funnels)
FROM (SELECT funnel(step, array(0,1,2,3)) as funnels
      ,funnel_first(state) AS state,funnel_first(CAST(logged_in AS STRING)) AS logged_in
      FROM (
          SELECT FIRST_VALUE((IF(step == 0, state, null)), TRUE) OVER (PARTITION BY user_id ORDER BY ts ASC) AS state,
FIRST_VALUE((IF(step == 0, logged_in, null)), TRUE) OVER (PARTITION BY user_id ORDER BY ts ASC) AS logged_in,
            step, user_id, ts
          FROM (
            SELECT CASE  WHEN ((
  user_event == 'value1' AND
  cookie_one is not NULL
)) THEN 0
  WHEN ((
  user_event == 'value2' OR
  user_event == 'sth_else'
)) THEN 1
  WHEN ((
  user_event == 'event3' AND
  (
    cookie_one_age == '10' AND
    cookie_one_info is not NULL
  )
)) THEN 2
  WHEN (user_event == 'event4') THEN 3
                        ELSE NULL
                   END AS step,
                   geo_info['state'] AS state,user_logged_in AS logged_in,
                   timestamp AS ts,
                   cookie_one AS user_id
            FROM schema1.daily_data
            WHERE (
  browser == 'browser1' AND
  cookie_one is not NULL
) AND  (((
  user_event == 'value1' AND
  cookie_one is not NULL
)) OR
 ((
  user_event == 'value2' OR
  user_event == 'sth_else'
)) OR
 ((
  user_event == 'event3' AND
  (
    cookie_one_age == '10' AND
    cookie_one_info is not NULL
  )
)) OR
 (user_event == 'event4')) AND
                  dt >= '20200507' AND dt < '20200508'
            DISTRIBUTE BY user_id
            SORT BY user_id, ts, step ASC) AS sorted_data_for_funnel
          DISTRIBUTE BY user_id
          SORT BY user_id, ts, step ASC) AS projected_data_for_funnel
      GROUP BY user_id) grouped_data_for_funnel
GROUP BY state,logged_in, funnels
