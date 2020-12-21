/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.settings;

import com.beust.jcommander.Parameter;
import java.lang.reflect.Field;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper methods to parse CLI args.
 */
@Slf4j
public class CLISettings {
    /** Help message. */
    @Parameter(names = "--help", help = true, description = "Prints this help message.")
    public static boolean HELP = false;

    /** Version string. */
    @Parameter(names = "--version", description = "Version string.")
    public static String VERSION = "0.0.0";

    /** User name used for managing pipelines. */
    @Parameter(names = "--user-name", description = "User name used for managing pipelines.")
    public static String USER_NAME = "cubed_user";

    /** Directory of schema files path. */
    @Parameter(names = "--schema-files-dir", description = "Schema definition file.", required = true)
    public static String SCHEMA_FILES_DIR = "src/test/resources/schemas/";

    /** Db config file path. */
    @Parameter(names = "--db-config-file", description = "Database configuration file.", required = true)
    public static String DB_CONFIG_FILE = "src/main/resources/database-configuration.properties";

    /** Local path for the scripts used for starting and stopping a pipeline. */
    @Parameter(names = "--pipeline-scripts-path", description = "Pipeline start and stop scripts path.")
    public static String PIPELINE_SCRIPTS_PATH = "src/main/resources/bin/";

    /** Local path for the Sketches Hive jar. */
    @Parameter(names = "--sketches-hive-jar-path", description = "Sketches Hive jar path")
    public static String SKETCHES_HIVE_JAR_PATH = "lib/";

    /** Name of the cubed instance. */
    @Parameter(names = "--instance-name", description = "Cubed instance name.")
    public static String INSTANCE_NAME = "cubed";

    /** Pipeline owner. */
    @Parameter(names = "--pipeline-owner", description = "Pipeline owner name (prod, staging, username, etc).")
    public static String PIPELINE_OWNER = "prod";

    /** HDFS output path for data. */
    @Parameter(names = "--hdfs-output-path", description = "HDFS output path for data.")
    public static String HDFS_OUTPUT_PATH = "/projects/cubed";

    /** HDFS path for Oozie pipeline configurations. */
    @Parameter(names = "--pipeline-deploy-path", description = "HDFS Path for Oozie pipeline configurations.")
    public static String PIPELINE_DEPLOY_PATH = "/user/cubed_user/cubed";

    /** HDFS trash directory. */
    @Parameter(names = "--trash-dir", description = "HDFS trash directory.")
    public static String TRASH_DIR = "hdfs:///user/cubed_user/.Trash";

    /** Hadoop job tracker URL. */
    @Parameter(names = "--job-tracker-url", description = "Hadoop job tracker URL.")
    public static String JOB_TRACKER_URL = "jobtracker.cubed.com:8080";

    /** Hadoop name node URL. */
    @Parameter(names = "--name-node-url", description = "Hadoop name node URL.")
    public static String NAME_NODE_URL = "hdfs://namenode.cubed.com:8080";

    /** HCatalog URI. */
    @Parameter(names = "--hcat-uri", description = "HCatalog URI.")
    public static String HCAT_URI = "hcat://hcatalog.cubed.com:8080";

    /** HCatalog principal. */
    @Parameter(names = "--hcat-principal", description = "HCatalog principal.")
    public static String HCAT_PRINCIPAL = "hcat/hcat.cubed.com@random.com";

    /** Oozie UI URL. */
    @Parameter(names = "--oozie-ui-url", description = "Oozie UI URL.")
    public static String OOZIE_UI_URL = "https://oozie.cubed.com:8080/oozie";

    /** Hive JDBC URL. */
    @Parameter(names = "--hive-jdbc-url", description = "Hive JDBC URL.")
    public static String HIVE_JDBC_URL = "jdbc:hive2://hive.cubed.com:8080/default";

    /** Oozie URL. */
    @Parameter(names = "--oozie-url", description = "Oozie URL.")
    public static String OOZIE_URL = "https://oozie.cubed.com:8080/oozie";

    /** Druid indexer location. */
    @Parameter(names = "--druid-indexer", description = "Druid indexer host.")
    public static String DRUID_INDEXER = "cubed-indexer.cubed.com:8080";

    /** Druid HTTP proxy used for loading data into Druid. */
    @Parameter(names = "--druid-http-proxy", description = "Druid HTTP proxy used for loading data into Druid.")
    public static String DRUID_HTTP_PROXY = "http://httpproxy.cubed.com:8080";

    /** Turnilo URL. */
    @Parameter(names = "--turnilo-url", description = "Turnilo URL.")
    public static String TURNILO_URL = "http://turnilo.cubed.com:8080/#%s_%s";

    /** Superset URL. */
    @Parameter(names = "--superset-url", description = "Superset URL.")
    public static String SUPERSET_URL = "http://superset.cubed.com";

    /** Cardinality cap for bullet. */
    @Parameter(names = "--cardinality-cap", description = "Cap of Distinct Count Numbers from Bullet Query.")
    public static int CARDINALITY_CAP = 50;

    /** Remote cluster hdfs servers to support. */
    @Parameter(names = "--hdfs-servers", description = "Remote cluster hdfs servers to support.")
    public static String HDFS_SERVERS = "hdfs://hdfs-servers.cubed.com";

    /** Port used when running in embedded web server mode. */
    @Parameter(names = "--port", description = "Port used when running in embedded web server mode.")
    public static int PORT = 9999;

    /** Number of threads used when running in embedded web server mode. */
    @Parameter(names = "--threads", description = "Number of threads used when running in embedded web server mode.")
    public static int THREADS = 200;

    /** Datamart and funnel template output folder. */
    @Parameter(names = "--template-output-folder", description = "Datamart and funnel template output folder.")
    public static String TEMPLATE_OUTPUT_FOLDER = "/tmp";

    /** Email to recieve pipeline failures. */
    @Parameter(names = "--pipeline-email", description = "Email to recieve pipeline failures.")
    public static String PIPELINE_EMAIL = "this-email-does-not-exist@cubed.com";

    /** Queue to use for pipelines. */
    @Parameter(names = "--queue", description = "Queue to use for pipelines.")
    public static String QUEUE = "adhoc";

    /** Bullet query Duration in Seconds. */
    @Parameter(names = "--bullet-query-duration", description = "Bullet query Duration in Seconds.")
    public static int BULLET_QUERY_DURATION = 15;

    /**
     * Print all the settings.
     */
    public void print() {
        // Get all fields in the class
        Field[] fields = this.getClass().getDeclaredFields();
        // Print the settings
        log.info("Printing all CLI settings...");
        for (Field field : fields) {
            try {
                // Print all columns (skip "log" and "HELP")
                if (field.getName() != "log" && field.getName() != "HELP") {
                    log.info("{}: {}", field.getName(), field.get(this));
                }
            } catch (Exception e) {
                log.error("Error getting content of {}", field.getName());
            }
        }
        log.info("Done printing all settings...");
    }
}
