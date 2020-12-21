/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.settings.CLISettings;

/**
 * Template file interface.
 * @param <T> Type.
 */
public interface TemplateFile<T> {

    /** Default xml indent used for pretty print. */
    int DEFAULT_XML_INDENT = 2;

    /** Product name attribute. */
    String PRODUCT_NAME_ATTRIBUTE = "PRODUCT_NAME";
    /** Version attribute. */
    String VERSION_ATTRIBUTE = "VERSION";
    /** HDFS trash directory. */
    String TRASH_DIR_ATTRIBUTE = "TRASH_DIR";
    /** Hadoop job tracker URL. */
    String JOB_TRACKER_URL_ATTRIBUTE = "JOB_TRACKER_URL";
    /** Hadoop name node URL. */
    String NAME_NODE_URL_ATTRIBUTE = "NAME_NODE_URL";
    /** HCatalog URI. */
    String HCAT_URI_ATTRIBUTE = "HCAT_URI";
    /** HCatalog principal. */
    String HCAT_PRINCIPAL_ATTRIBUTE = "HCAT_PRINCIPAL";
    /** Oozie UI URL. */
    String OOZIE_UI_URL_ATTRIBUTE = "OOZIE_UI_URL";
    /** User name used for managing pipelines. */
    String USER_NAME_ATTRIBUTE = "USER_NAME";
    /** Queue to use for pipelines. */
    String QUEUE_ATTRIBUTE = "QUEUE";
    /** Email to recieve pipeline failures. */
    String EMAIL_ATTRIBUTE = "EMAIL";
    /** HDFS path for Oozie pipeline configurations. */
    String DEPLOY_PATH_ATTRIBUTE = "DEPLOY_PATH";
    /** Pipeline owner. */
    String PIPELINE_OWNER_ATTRIBUTE = "PIPELINE_OWNER";
    /** Table attribute. */
    String TABLE_ATTRIBUTE = "TABLE";
    /** DB output path attribute. */
    String DB_OUTPUT_PATH_ATTRIBUTE = "DB_OUTPUT_PATH";
    /** Remote cluster hdfs servers to support. */
    String HDFS_SERVERS_ATTRIBUTE = "HDFS_SERVERS";
    /** Funnel end time attribute. */
    String FUNNEL_END_TIME = "FUNNEL_END_TIME";
    /** In database attribute. */
    String IN_DATABASE = "IN_DATABASE";
    /** In hour table attribute. */
    String IN_HOUR_TABLE = "IN_HOUR_TABLE";
    /** In hour partition attribute. */
    String IN_HOUR_PARTITION = "IN_HOUR_PARTITION";
    /** In day table attribute. */
    String IN_DAY_TABLE = "IN_DAY_TABLE";
    /** In day partition attribute. */
    String IN_DAY_PARTITION = "IN_DAY_PARTITION";
    /** Target table attribute used for funnel ad-hoc queries. */
    String TARGET_TABLE = "TARGET_TABLE";
    /** Pipeline deploy path template. */
    String DEPLOY_PATH_TEMPLATE = CLISettings.PIPELINE_DEPLOY_PATH + "/%s_v%s";
    /** Zeroth hour format. */
    String ZERO_HOUR = "T00:00Z";
    /** Oozie pipeline default end time date attribute. */
    String DEFAULT_END_TIME_DATE = "2100-01-01" + ZERO_HOUR;
    /** Druid HTTP proxy attribute. */
    String DRUID_HTTP_PROXY_ATTRIBUTE = "DRUID_HTTP_PROXY";
    /** Druid indexer attribute. */
    String DRUID_INDEX_ATTRIBUTE = "DRUID_INDEX";
    /** Granularity attribute. */
    String GRANULARITY_ATTRIBUTE = "GRANULARITY_PARAMETER";
    /** Bundle property file. */
    public static final String BUNDLE_PROPERTY_FILE_ATTRIBUTE = "BUNDLE_PROPERTY_FILE";
    /** Funnel query range. */
    public static final String FUNNEL_QUERY_RANGE = "FUNNEL_QUERY_RANGE";
    /** Funnel repeat interval. */
    public static final String FUNNEL_REPEAT_INTERVAL = "FUNNEL_REPEAT_INTERVAL";
    /** Funnel steps. */
    public static final String FUNNEL_STEPS = "FUNNEL_STEPS";

    /**
     * Generates a template file given a model and version.
     * @param model
     * @param version
     */
    public abstract String generateFile(T model, long version) throws Exception;
}
