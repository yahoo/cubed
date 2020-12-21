SET hive.exec.compress.output=false;
FROM ${hivevar:IN_DATABASE_AND_TABLE}
INSERT OVERWRITE DIRECTORY '${hivevar:OUT_DIR}'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS textfile
SELECT
       (unix_timestamp('${hivevar:YEAR}-${hivevar:MONTH}-${hivevar:DAY} ${hivevar:HOUR}:01:00') * 1000),
       count(1),
       COALESCE(regexp_replace(string_field_1, '[\\t\\r\\n]', ''), ''),
       bool_map_field_1['key_1'],
       COALESCE(regexp_replace(string_map_field_1['_fieldkey3'], '[\\t\\r\\n]', ''), ''),
       COALESCE(SUM(integer_column), 0),
       COALESCE(base64(data_to_sketch(string_field_2, 2048, 1.0)), '')
WHERE
      (
        string_field_1 > '3' OR 
        bool_map_field_6 is NULL OR 
        bool_map_field_6 is not NULL OR 
        (
          bool_map_field_1['_fieldkey2'] != false AND 
          string_map_field_1 is not NULL
        ) OR 
        (
          string_map_field_1 == 'abc' AND 
          string_map_field_2['_filterkey5'] != 'def'
        )
      ) AND 
      dt = '${hivevar:DATE_FILTER}'
GROUP BY string_field_1,bool_map_field_1['key_1'],string_map_field_1['_fieldkey3'];
