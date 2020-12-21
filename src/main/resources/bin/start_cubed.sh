#!/bin/bash

# Configurations
VERSION=$(VERSION)
USER_NAME=$(USER_NAME)
SCHEMA_FILES_DIR=$(SCHEMA_FILES_DIR)
DB_CONFIG_FILE=$(DB_CONFIG_FILE)
PIPELINE_SCRIPTS_PATH=$(PIPELINE_SCRIPTS_PATH)
SKETCHES_HIVE_JAR_PATH=$(SKETCHES_HIVE_JAR_PATH)
INSTANCE_NAME=$(INSTANCE_NAME)
PIPELINE_OWNER=$(PIPELINE_OWNER)
HDFS_OUTPUT_PATH=$(HDFS_OUTPUT_PATH)
PIPELINE_DEPLOY_PATH=$(PIPELINE_DEPLOY_PATH)
TRASH_DIR=$(TRASH_DIR)
JOB_TRACKER_URL=$(JOB_TRACKER_URL)
NAME_NODE_URL=$(NAME_NODE_URL)
HCAT_URI=$(HCAT_URI)
HCAT_PRINCIPAL=$(HCAT_PRINCIPAL)
OOZIE_UI_URL=$(OOZIE_UI_URL)
HIVE_JDBC_URL=$(HIVE_JDBC_URL)
OOZIE_URL=$(OOZIE_URL)
DRUID_INDEXER=$(DRUID_INDEXER)
DRUID_HTTP_PROXY=$(DRUID_HTTP_PROXY)
TURNILO_URL=$(TURNILO_URL)
SUPERSET_URL=$(SUPERSET_URL)
CARDINALITY_CAP=$(CARDINALITY_CAP)
HDFS_SERVERS=$(HDFS_SERVERS)
PORT=$(PORT)
THREADS=$(THREADS)
TEMPLATE_OUTPUT_FOLDER=$(TEMPLATE_OUTPUT_FOLDER)
PIPELINE_EMAIL=$(PIPELINE_EMAIL)
QUEUE=$(QUEUE)
BULLET_QUERY_DURATION=$(BULLET_QUERY_DURATION)

# Path constants
HADOOP_HOME=$(HADOOP_HOME)
HIVE_HOME=$(HIVE_HOME)
HIVE_JDBC_HOME=$(HIVE_JDBC_HOME)
HADOOP_COMMON=$(HADOOP_COMMON)
CLASSPATH=$(CLASSPATH)
LOG_PROPERTIES_FILE=$(LOG_PROPERTIES_FILE)

# The pid file location
pid_file=$(PID_FILE)

# Check if already running
if [ -f "$pid_file" ]
then
    echo "Already running cubed, try running 'stop_cubed' first."
else
    # Use backup database if exist
    if [ -f "$(DB_FILE_PATH).mv.db" ]
    then
        cp $(DB_FILE_PATH).mv.db cubed_`date +'%Y%m%d%H%M%S'`
    fi

    for i in ${HADOOP_HOME}/*.jar;  do
        CLASSPATH=$CLASSPATH:$i
    done

    for i in ${HADOOP_COMMON}/*.jar;  do
        CLASSPATH=$CLASSPATH:$i
    done

    for i in ${HIVE_HOME}/lib/*.jar ; do
        CLASSPATH=$CLASSPATH:$i
    done

    for i in ${HIVE_JDBC_HOME}/lib/*.jar ; do
        CLASSPATH=$CLASSPATH:$i
    done

    echo "Classpath is $CLASSPATH"

    # Launch the service
    java  -Dlog4j.configuration=file:$LOG_PROPERTIES_FILE \
          -Djavax.security.auth.useSubjectCredsOnly=false \
          -cp "$CLASSPATH" \
          com.yahoo.cubed.App \
          --version "$VERSION" \
          --user-name "$USER_NAME" \
          --schema-files-dir "$SCHEMA_FILES_DIR" \
          --db-config-file "$DB_CONFIG_FILE" \
          --pipeline-scripts-path "$PIPELINE_SCRIPTS_PATH" \
          --sketches-hive-jar-path "$SKETCHES_HIVE_JAR_PATH" \
          --instance-name "$INSTANCE_NAME" \
          --pipeline-owner "$PIPELINE_OWNER" \
          --hdfs-output-path "$HDFS_OUTPUT_PATH" \
          --pipeline-deploy-path "$PIPELINE_DEPLOY_PATH" \
          --trash-dir "$TRASH_DIR" \
          --job-tracker-url "$JOB_TRACKER_URL" \
          --name-node-url "$NAME_NODE_URL" \
          --hcat-uri "$HCAT_URI" \
          --hcat-principal "$HCAT_PRINCIPAL" \
          --oozie-ui-url "$OOZIE_UI_URL" \
          --hive-jdbc-url "$HIVE_JDBC_URL" \
          --oozie-url "$OOZIE_URL" \
          --druid-indexer "$DRUID_INDEXER" \
          --druid-http-proxy "$DRUID_HTTP_PROXY" \
          --turnilo-url "$TURNILO_URL" \
          --superset-url "$SUPERSET_URL" \
          --cardinality-cap "$CARDINALITY_CAP" \
          --hdfs-servers "$HDFS_SERVERS" \
          --port "$PORT" \
          --threads "$THREADS" \
          --template-output-folder "$TEMPLATE_OUTPUT_FOLDER" \
          --pipeline-email "$PIPELINE_EMAIL" \
          --queue "$QUEUE" \
          --bullet-query-duration "$BULLET_QUERY_DURATION" \
          >/dev/null 2>&1 &

    # Store the pid
    echo $! > $pid_file
fi
