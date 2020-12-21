<?xml version="1.0"?>
<!--
  ~ Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
  -->
<workflow-app xmlns="uri:oozie:workflow:0.5" xmlns:sla="uri:oozie:sla:0.2" name="\${product_name}_\${owner}_day_\${year}-\${month}-\${day}">
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
  <start to="load_into_druid"/>
  <!-- Don't run if the Druid TSV data is less than 1 byte-->
  <!-- Also captures -1, dir does not exist-->
  <action name="load_into_druid">
    <shell xmlns="uri:oozie:shell-action:0.3">
      <job-tracker>\${job_tracker}</job-tracker>
      <name-node>\${name_node}</name-node>
      <job-xml>job.xml</job-xml>
      <exec>bash</exec>
      <argument>druid_load.sh</argument>
      <argument>\${year}</argument>
      <argument>\${month}</argument>
      <argument>\${day}</argument>
      <!-- Number of minute to wait for Druid to finish loading the data-->
      <argument>180</argument>
      <file>druid_load.sh#druid_load.sh</file>
      <file>index.json#index.json</file>
    </shell>
    <ok to="delete_tsv_data"/>
    <error to="send_email"/>
  </action>
  <!-- Druid load Completed successfully, delete TSV data. -->
  <action name="delete_tsv_data">
    <fs>
      $TSV_DIR_TO_DELETE$
    </fs>
    <ok to="end"/>
    <error to="send_email"/>
  </action>
  <action name="send_email">
    <sub-workflow>
      <app-path>\${project_path}/email/workflow.xml</app-path>
      <propagate-configuration/>
      <configuration>
        <property>
          <name>workflow_last_error_node</name>
          <value>\${firstNotNull(wf:lastErrorNode(), "UNKNOWN")}</value>
        </property>
        <property>
          <name>workflow_id</name>
          <value>\${firstNotNull(wf:id(), "UNKNOWN")}</value>
        </property>
        <property>
          <name>workflow_name</name>
          <value>\${firstNotNull(wf:name(), "UNKNOWN")}</value>
        </property>
        <property>
          <name>workflow_action_id</name>
          <value>\${firstNotNull(wf:actionExternalId(wf:lastErrorNode()), "UNKNOWN")}</value>
        </property>
        <property>
          <name>workflow_error_code</name>
          <value>\${firstNotNull(wf:errorCode(wf:lastErrorNode()), "UNKNOWN")}</value>
        </property>
        <property>
          <name>workflow_error_message</name>
          <value>\${firstNotNull(wf:errorMessage(wf:lastErrorNode()), "UNKNOWN")}</value>
        </property>
      </configuration>
    </sub-workflow>
    <ok to="end"/>
    <error to="fail"/>
  </action>
  <kill name="fail">
    <message>Failed Oozie pipeline.</message>
  </kill>
  <end name="end"/>
</workflow-app>
