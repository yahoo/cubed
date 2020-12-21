add jar sketches-hive-0.13.0-with-shaded-core.jar;
create temporary function unionSketches as 'com.yahoo.sketches.hive.theta.UnionSketchUDAF';

SET hive.exec.compress.output=false;

use funnel;
create temporary table theta_input($FINAL_PROJECTIONS_WITH_TYPE$ step int, sketch binary);
insert into theta_input
SELECT $FINAL_PROJECTIONS$
       funnels, data_to_sketch(user_id, 262144)
FROM (SELECT user_id, funnel(step, array($NUM_STEPS$)) as funnels
      $OUTER_SELECT_PROJECTIONS$
      FROM (
            SELECT $WINDOW_FUNCTIONS$
              step, user_id, ts
            FROM (
                  SELECT CASE $STEPS$
                              ELSE NULL
                         END AS step,
                         $INNER_SELECT_PROJECTIONS$
                         $TIMESTAMP_COLUMN$ AS ts,
                         $USER_ID_COLUMN$ AS user_id
                  FROM \${hivevar:IN_DATABASE_AND_TABLE}
                  WHERE $FILTERS$ $STEP_FILTERS$
                        dt >= '\${hivevar:QUERY_START_DATE}' AND dt < '\${hivevar:QUERY_END_DATE}'
                        $SAMPLING$
                  DISTRIBUTE BY user_id
                  SORT BY user_id, ts, step ASC) AS sorted_data_for_funnel
            DISTRIBUTE BY user_id
            SORT BY user_id, ts, step ASC) AS projected_data_for_funnel
      GROUP BY user_id) AS grouped_data_for_funnel
$FINAL_PROJECTIONS_GROUP_BY$;

create temporary table sketch_intermediate($FINAL_PROJECTIONS_WITH_TYPE$ step string, prev_step string, funnel_name string, sketch binary);
$GENERATE_SKETCHES$

INSERT OVERWRITE DIRECTORY '\${hivevar:OUT_DIR}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
NULL DEFINED AS 'AgMDAAAazJMBAAAAAACAP3ut7t17mCt2'
STORED AS textfile
select '\${hivevar:QUERY_START_DATE}', $FINAL_PROJECTIONS_COALESCE$ COALESCE(step, 'null'), COALESCE(prev_step, 'null'), COALESCE(funnel_name, 'null'), sketch from sketch_intermediate;