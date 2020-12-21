SELECT $FINAL_PROJECTIONS$
       funnel_merge(funnels)
FROM (SELECT funnel(step, array($NUM_STEPS$)) as funnels
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
            FROM $IN_DATABASE$.$TARGET_TABLE$
            WHERE $FILTERS$ $STEP_FILTERS$
                  dt >= '$START_DATE$' AND dt < '$END_DATE$'
                  $SAMPLING$
            DISTRIBUTE BY user_id
            SORT BY user_id, ts, step ASC) AS sorted_data_for_funnel
          DISTRIBUTE BY user_id
          SORT BY user_id, ts, step ASC) AS projected_data_for_funnel
      GROUP BY user_id) grouped_data_for_funnel
$FINAL_PROJECTIONS_GROUP_BY$
