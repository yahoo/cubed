SET hive.exec.compress.output=false;
FROM \${hivevar:IN_DATABASE_AND_TABLE}
INSERT OVERWRITE DIRECTORY '\${hivevar:OUT_DIR}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS textfile
SELECT
       (unix_timestamp('\${hivevar:YEAR}-\${hivevar:MONTH}-\${hivevar:DAY} \${hivevar:HOUR}:01:00') * 1000),
       count(1),
       $transformations$
WHERE
      $filters$
      $datetime_partition_column$ = '\${hivevar:DATE_FILTER}'
$group_by_option$;
