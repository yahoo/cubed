<?xml version="1.0"?>
<!--
  ~ Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
  -->
<workflow-app xmlns="uri:oozie:workflow:0.5" xmlns:sla="uri:oozie:sla:0.2" name="$WORKFLOW_NAME$_\${owner}_day_\${year}-\${month}-\${day}">
    <global>
        <configuration>
          <property>
            <name>oozie.launcher.mapreduce.job.hdfs-servers</name>
            <value>\${hdfs_servers}</value>
          </property>
        </configuration>
    </global>
    <credentials>
        <credential name="hcatauth" type="hcat">
            <property>
                <name>hcat.metastore.uri</name>
                <value>\${hcat_uri}</value>
            </property>
            <property>
                <name>hcat.metastore.principal</name>
                <value>\${hcat_principal}</value>
            </property>
        </credential>
    </credentials>
    <start to="query"/>
    <action name="query" cred="hcatauth" retry-max="3" retry-interval="2">
        <hive xmlns="uri:oozie:hive-action:0.5">
            <job-tracker>\${job_tracker}</job-tracker>
            <name-node>\${name_node}</name-node>
            <prepare>
                <delete path='\${out_dir}'/>
            </prepare>
            <job-xml>job.xml</job-xml>
            <configuration>
                <property>
                    <name>mapreduce.job.queuename</name>
                    <value>\${queue}</value>
                </property>
                <property>
                    <name>hive.metastore.client.socket.timeout</name>
                    <value>200</value>
                </property>
                <property>
                    <name>oozie.action.sharelib.for.hive</name>
                    <value>hive_current,hcat_current</value>
                </property>
            </configuration>
            <script>\${script_path}/query_$FUNNEL_NAME$.hql</script>
            <param>IN_DATABASE_AND_TABLE=\${in_database}.\${in_day_table}</param>
            <param>OUT_DIR=\${out_dir}</param>
            <param>GRANULARITY=day</param>
            <param>YEAR=\${year}</param>
            <param>MONTH=\${month}</param>
            <param>DAY=\${day}</param>
            <param>HOUR=00</param>
            <param>QUERY_START_DATE=\${query_start_date}</param>
            <param>QUERY_END_DATE=\${query_end_date}</param>
            <file>lib/sketches-hive-0.13.0-with-shaded-core.jar#sketches-hive-0.13.0-with-shaded-core.jar</file>
        </hive>
        <ok to="end"/>
        <error to="fail"/>
    </action>
    <kill name="fail">
        <message>Failed Oozie pipeline.</message>
    </kill>
    <end name="end"/>
</workflow-app>
