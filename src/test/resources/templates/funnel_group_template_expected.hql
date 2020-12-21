add jar sketches-hive-0.13.0-with-shaded-core.jar;
create temporary function unionSketches as 'com.yahoo.sketches.hive.theta.UnionSketchUDAF';

SET hive.exec.compress.output=false;

use funnel;
create temporary table theta_input(state string,logged_in string, step int, sketch binary);
insert into theta_input
SELECT state,logged_in,
       funnels, data_to_sketch(user_id, 262144)
FROM (SELECT user_id, funnel(step, array(0,1,2,3)) as funnels
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
                  FROM ${hivevar:IN_DATABASE_AND_TABLE}
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
                        dt >= '${hivevar:QUERY_START_DATE}' AND dt < '${hivevar:QUERY_END_DATE}'
                  DISTRIBUTE BY user_id
                  SORT BY user_id, ts, step ASC) AS sorted_data_for_funnel
            DISTRIBUTE BY user_id
            SORT BY user_id, ts, step ASC) AS projected_data_for_funnel
      GROUP BY user_id) AS grouped_data_for_funnel
GROUP BY state,logged_in, funnels;

create temporary table sketch_intermediate(state string,logged_in string, step string, prev_step string, funnel_name string, sketch binary);
insert into sketch_intermediate select state,logged_in, 'step1', 'FUNNEL_START', 'test_funnel', unionSketches(sketch, 262144) from theta_input where step >= 1 group by state,logged_in;
insert into sketch_intermediate select state,logged_in, 'step2', 'step1', 'test_funnel', unionSketches(sketch, 262144) from theta_input where step >= 2 group by state,logged_in;
insert into sketch_intermediate select state,logged_in, 'step3', 'step2', 'test_funnel', unionSketches(sketch, 262144) from theta_input where step >= 3 group by state,logged_in;
insert into sketch_intermediate select state,logged_in, 'step4', 'step3', 'test_funnel', unionSketches(sketch, 262144) from theta_input where step >= 4 group by state,logged_in;


INSERT OVERWRITE DIRECTORY '${hivevar:OUT_DIR}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
NULL DEFINED AS 'AgMDAAAazJMBAAAAAACAP3ut7t17mCt2'
STORED AS textfile
select '${hivevar:QUERY_START_DATE}', COALESCE(regexp_replace(state, '[\\t\\r\\n]', ''), 'null'),COALESCE(regexp_replace(logged_in, '[\\t\\r\\n]', ''), 'null'), COALESCE(step, 'null'), COALESCE(prev_step, 'null'), COALESCE(funnel_name, 'null'), sketch from sketch_intermediate;
