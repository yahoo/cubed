<?xml version="1.0"?>
<!--
  ~ Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
  -->
<coordinator-app xmlns="uri:oozie:coordinator:0.4" name="\${product_name}_\${owner}_funnel" frequency="\${coord:days(1)}" start="\${funnel_start_time}" end="\${funnel_end_time}" timezone="UTC">
    <controls>
        <timeout>-1</timeout>
        <concurrency>\${parallel_day}</concurrency>
        <execution>FIFO</execution>
    </controls>
    <datasets>
        $INPUT_DATASETS$
        <dataset name="out_dir" frequency="\${coord:days(1)}" initial-instance="\${funnel_start_time}" timezone="UTC">
            <uri-template>hdfs://\${hdfs_output_path}/\${table}_\${YEAR}_\${MONTH}_\${DAY}</uri-template>
        </dataset>
    </datasets>
    <input-events>
        $INPUT_EVENTS$
    </input-events>
    <output-events>
        <data-out name="OUT_DIR" dataset="out_dir">
            <instance>\${coord:current(0)}</instance>
        </data-out>
    </output-events>
    <action>
        <workflow>
            <app-path>\${project_path}/funnel_group/workflow.xml</app-path>
            <configuration>
                <property>
                    <name>out_dir</name>
                    <value>\${coord:dataOut('OUT_DIR')}</value>
                </property>
                <property>
                    <name>year</name>
                    <value>\${coord:formatTime(coord:nominalTime(),'yyyy')}</value>
                </property>
                <property>
                    <name>month</name>
                    <value>\${coord:formatTime(coord:nominalTime(),'MM')}</value>
                </property>
                <property>
                    <name>day</name>
                    <value>\${coord:formatTime(coord:nominalTime(),'dd')}</value>
                </property>
                <property>
                    <name>hour</name>
                    <value>\${coord:formatTime(coord:nominalTime(),'HH')}</value>
                </property>
                <property>
                    <name>nominal_time</name>
                    <value>\${coord:nominalTime()}</value>
                </property>
                <property>
                    <name>query_start_date</name>
                    <value>\${coord:formatTime(coord:nominalTime(),'yyyyMMdd')}</value>
                </property>
                <property>
                    <name>query_end_date</name>
                    <value>\${coord:formatTime(coord:dateOffset(coord:nominalTime(), 1, 'DAY'),'yyyyMMdd')}</value>
                </property>
            </configuration>
        </workflow>
    </action>
</coordinator-app>
