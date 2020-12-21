set +e

# Customize settings
function exportSetting {
    export HEADLESS_USER=$(USER_NAME)
    export DEST_FOLDER=$(DEST_FOLDER)
    export DEPLOY_PATH="${DEST_FOLDER}/${PIPELINE_NAME}_v${PIPELINE_VERSION}"
    export INSTANCE_NAME=$(INSTANCE_NAME)
    export PRODUCT_NAME="${INSTANCE_NAME}_${PIPELINE_NAME}"
    export DRUID_COORD=$(DRUID_COORD)
    export OOZIE_URL=$(OOZIE_URL)
}

function deployPipeline {

    # ------------- (1) check deployment path -------------------
    printInfo "Check if deploy already exists: ${DEPLOY_PATH}"
    RESULT=`hdfs_ls ${DEPLOY_PATH}`
    if [ "$RESULT" == "found on hdfs" ]; then
          printError "Deploy already exists ${DEPLOY_PATH}. Terminating deploy."
          exit 1
    fi

    printInfo "We have not deployed this before, proceeding."
    printInfo "We assume hive_conf package already installed for the grid we are on."


    # -------------- (2) search for existing (old) jobs -------------------
    # Get all running or waiting jobs matching the product name/owner combination
    printInfo "Searching for jobs named ${PRODUCT_NAME}_${PIPELINE_OWNER}..."
    RETURNED_JOBS=`oozie_jobSearch bundle ${PRODUCT_NAME}_${PIPELINE_OWNER} ${OOZIE_URL}`
    printInfo "Searching result of returned jobs: ${RETURNED_JOBS}"

    # Delete any previous datasource (even there is none)
    curl -v -L -X 'DELETE' http://${DRUID_COORD}/druid/coordinator/v1/datasources/${PRODUCT_NAME} >/dev/null 2>&1

    if [ -z "${RETURNED_JOBS}" ]; then
        printInfo "no previous running jobs"
    else
        for job_id in ${RETURNED_JOBS}; do
            printInfo "    Kill previous job ${job_id}..."
            oozie_jobKill ${job_id} ${OOZIE_URL}
            printInfo "    Done kill previous job ${job_id}."
        done
        printInfo "Done kill all previous jobs."
        printInfo "Dropped previous Druid datasource ${PRODUCT_NAME}."
    fi

    # -------------- (3) set pipeline start time ---------------
    # Check if there are active running bundles. If so, start tomorrow at 00
    # Else, start today at 00
    if [ "${FUNNEL_START_PARAM}" = '0' ]; then
        printError "Invalid funnel start date"
        exit 1
    else
        FUNNEL_START_TIME=`echo ${FUNNEL_START_PARAM} | sed -e 's/^\([0-9]\{4\}\)-\([0-9]\{2\}\)-\([0-9]\{2\}\).*$/\1-\2-\3T00:00Z/g'`
        printInfo "START TIME: ${FUNNEL_START_TIME}."
    fi


    # -------------- (4) set funnel pipeline start time---------------

    # Set the day start time to be the same as hour for backfill
    sed -i -e 's/FUNNEL_START_TIME/'"${FUNNEL_START_TIME}"'/g' ${SRC_FOLDER}/properties.xml
    printInfo "Set FUNNEL_START_TIME: ${FUNNEL_START_TIME}"


    # -------------- (5) Push files to HDFS --------------------------
    printInfo "Pushing the pipeline from ${SRC_FOLDER} to the grid on ${DEST_FOLDER} ..."
    HDFS_PUT_RESULT=`hdfs_folder_put ${SRC_FOLDER} ${DEST_FOLDER}`
    printInfo "Done pushing the pipeline to the grid with result ${HDFS_PUT_RESULT}"

    # Check Result for Push files to HDFS
    if [ "${HDFS_PUT_RESULT}" != 'success' ]; then
        printError "Failed to put the resource files to HDFS: ${HDFS_PUT_RESULT}"
        exit 1
    fi


    # -------------- (6) lanuch the new pipeline ---------------
    printInfo "The new pipeline will be launched with the following times: ${FUNNEL_START_TIME}"

    # Launch the pipeline
    printInfo "Launching the pipeline..."
    JOB_SUBMIT_RESULT=`oozie_jobSubmit ${SRC_FOLDER}/properties.xml ${OOZIE_URL}`
    printInfo "Done launching the pipeline with result ${JOB_SUBMIT_RESULT}"

    # Check Result for Launch the pipeline
    if [[ "${JOB_SUBMIT_RESULT}" =~ "Error" ]]; then
        printError "Failed to submit the oozie job: ${JOB_SUBMIT_RESULT}"
        exit 1
    fi

    # Fetch oozie job id
    printOozieJobID "${JOB_SUBMIT_RESULT}"

    # -----------------------------------------------------------------------

    # Done
    printInfo "Done deploying and launching the pipeline."

    # Remove the source folder
    printInfo "Remove the ${SRC_FOLDER}"
    REMOVE_SRC_FOLDER=`rm -rf ${SRC_FOLDER}`
    printInfo "Done removing the folder with result ${REMOVE_SRC_FOLDER}"

    exit 0

}

############## MAIN ENTRY ###############

if [ $# -ne 7 ]; then
    printError "Must accept 7 arguments for funnel: name of the pipeline, its version, its owner, the path of the folder to be uploaded to HDFS, backfill start date, oozie job type, and oozie backfill job type."
    exit 1
fi

# if backfill start parameter is 0, it means no backfill


# Need these exports to avoid "hdfs command not found" error
# export PATH=$(PATH)
# export JAVA_HOME=$(JAVA_HOME)
# export HADOOP_HOME=$(HADOOP_HOME)
# export HADOOP_CONF_DIR=$(HADOOP_CONF_DIR)

export PIPELINE_NAME=`echo $1`
export PIPELINE_VERSION=`echo $2`
export PIPELINE_OWNER=`echo $3`
export SRC_FOLDER=`echo $4`
export FUNNEL_START_PARAM=`echo $5`

exportSetting
source common.sh

printInfo "------ PRINT SCRIPT INPUTS ------"
printInfo "Pipeline name is ${PIPELINE_NAME}"
printInfo "Pipeline version is ${PIPELINE_VERSION}"
printInfo "Pipeline owner is ${PIPELINE_OWNER}"
printInfo "Folder path is ${SRC_FOLDER}"
printInfo "Funnel start time parameter is ${FUNNEL_START_PARAM}"
printInfo "------ PRINT ENV VARS ------"
printInfo "HEADLESS_USER=${HEADLESS_USER}"
printInfo "DEPLOY_PATH=${DEPLOY_PATH}"
printInfo "DEST_FOLDER=${DEST_FOLDER}"
printInfo "INSTANCE_NAME=${INSTANCE_NAME}"
printInfo "PRODUCT_NAME=${PRODUCT_NAME}"
printInfo "DRUID_COORD=${DRUID_COORD}"
printInfo "OOZIE_URL=${OOZIE_URL}"
printInfo "------ DONE PRINTING ------"

deployPipeline
