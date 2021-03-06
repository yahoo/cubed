<?xml version="1.0"?>
<!--
  ~ Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
  -->
<configuration>
<property>
  <name>hdfs_servers</name>
  <value>$HDFS_SERVERS$</value>
</property>
<property>
  <name>product_version</name>
  <value>$VERSION$</value>
</property>
<property>
  <name>user.name</name>
  <value>$USER_NAME$</value>
</property>
<property>
  <name>trash_dir</name>
  <value>$TRASH_DIR$</value>
</property>
<property>
  <name>oozie.use.system.libpath</name>
  <value>true</value>
</property>
<property>
  <name>product_name</name>
  <value>$PRODUCT_NAME$</value>
</property>
<property>
  <name>project_path</name>
  <value>$DEPLOY_PATH$</value>
</property>
<property>
  <name>script_path</name>
  <value>\${project_path}/scripts</value>
</property>
<property>
  <name>owner</name>
  <value>$PIPELINE_OWNER$</value>
</property>
<property>
  <name>oozie.libpath</name>
  <value>\${project_path}/lib</value>
</property>
<property>
  <name>oozie.bundle.application.path</name>
  <value>\${project_path}/$BUNDLE_PROPERTY_FILE$.xml</value>
</property>
<property>
  <name>job_tracker</name>
  <value>$JOB_TRACKER_URL$</value>
</property>
<property>
  <name>name_node</name>
  <value>$NAME_NODE_URL$</value>
</property>
<property>
  <name>hcat_uri</name>
  <value>$HCAT_URI$</value>
</property>
<property>
  <name>hcat_principal</name>
  <value>$HCAT_PRINCIPAL$</value>
</property>
<property>
  <name>oozie_ui_url</name>
  <value>$OOZIE_UI_URL$</value>
</property>
<property>
  <name>hdfs_output_path</name>
  <value>$DB_OUTPUT_PATH$</value>
</property>
<property>
  <name>queue</name>
  <value>$QUEUE$</value>
</property>
<property>
  <name>table</name>
  <value>$TABLE$</value>
</property>
<property>
  <name>email</name>
  <value>$EMAIL$</value>
</property>
<property>
  <name>funnel_start_time</name>
  <value>FUNNEL_START_TIME</value>
</property>
<property>
  <name>funnel_end_time</name>
  <value>$FUNNEL_END_TIME$</value>
</property>
<property>
  <name>sla_hour_should_start</name>
  <value>120</value>
</property>
<property>
  <name>sla_hour_should_end</name>
  <value>480</value>
</property>
<property>
  <name>sla_hour_max_duration</name>
  <value>480</value>
</property>
<property>
  <name>parallel_hour</name>
  <value>3</value>
</property>
<property>
  <name>parallel_day</name>
  <value>3</value>
</property>
<property>
  <name>druid_parallel_hour</name>
  <value>1</value>
</property>
<property>
  <name>druid_parallel_day</name>
  <value>1</value>
</property>
<property>
  <name>funnel_query_range</name>
  <value>$FUNNEL_QUERY_RANGE$</value>
</property>
<property>
  <name>funnel_repeat_interval</name>
  <value>$FUNNEL_REPEAT_INTERVAL$</value>
</property>
<property>
  <name>in_database</name>
  <value>$IN_DATABASE$</value>
</property>
<property>
  <name>in_hour_table</name>
  <value>$IN_HOUR_TABLE$</value>
</property>
<property>
  <name>in_hour_partition</name>
  <value>$IN_HOUR_PARTITION$</value>
</property>
<property>
  <name>in_day_table</name>
  <value>$IN_DAY_TABLE$</value>
</property>
<property>
  <name>in_day_partition</name>
  <value>$IN_DAY_PARTITION$</value>
</property>
<property>
  <name>target_table</name>
  <value>$TARGET_TABLE$</value>
</property>
</configuration>
