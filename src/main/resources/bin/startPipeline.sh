set +e

# Customize settings
exportSetting() {
	export HEADLESS_USER=$(USER_NAME)
	export DEST_FOLDER=$(DEST_FOLDER)
	export DEPLOY_PATH="${DEST_FOLDER}/${PIPELINE_NAME}_v${PIPELINE_VERSION}"
	export INSTANCE_NAME=$(INSTANCE_NAME)
	export PRODUCT_NAME="${INSTANCE_NAME}_${PIPELINE_NAME}"
	export OOZIE_URL=$(OOZIE_URL)
}

deployPipeline() {

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

	# -------------- (3) set pipeline start time ---------------
	# Check if there are active running bundles. If so, start tomorrow at 00
    # Else, start today at 00
	if [ -z "${RETURNED_JOBS}" ]; then # no currently running bundles
	    REGULAR_START_TIME=`date +%Y-%m-%dT00:00Z -u`
    else # there are currently running bundles
	    REGULAR_START_TIME=`date +%Y-%m-%dT00:00Z -u -d '+1 day'`
    fi

	# Set the regular oozie job start time
	sed -i -e 's/REGULAR_START_TIME/'"${REGULAR_START_TIME}"'/g' ${SRC_FOLDER}/properties.xml
	printInfo "Set REGULAR_START_TIME: ${REGULAR_START_TIME}."
	
	# ------------- (4) check and prepare for backfill pipeline -------------
	if [ "${BACKFILL_START_PARAM}" = '0' ]; then
		BACKFILL_ELIGIBLE='false'
	else
        if [ -z "${RETURNED_JOBS}" ]; then # no currently running bundles
        	BACKFILL_ELIGIBLE='true'
			BACKFILL_START_TIME=`echo ${BACKFILL_START_PARAM} | sed -e 's/^\([0-9]\{4\}\)-\([0-9]\{2\}\)-\([0-9]\{2\}\).*$/\1-\2-\3T00:00Z/g'`
			BACKFILL_END_TIME=${REGULAR_START_TIME}
			printInfo "Backfill START TIME: ${BACKFILL_START_TIME}."
			printInfo "Backfill END TIME: ${BACKFILL_END_TIME}."
		else # there are currently running bundles
			BACKFILL_ELIGIBLE='false'
		fi
	fi
	
	printInfo "Prepared for backfill job. Eligible is ${BACKFILL_ELIGIBLE}."


	# -------------- (5) set backfill pipeline start time and end time ---------------
	if [ "${BACKFILL_ELIGIBLE}" = 'true' ]; then

		# Set the day start time to be the same as hour for backfill
		sed -i -e 's/BACKFILL_START_TIME/'"${BACKFILL_START_TIME}"'/g' ${SRC_FOLDER}/properties_backfill.xml
		printInfo "Set backfill BACKFILL_START_TIME: ${BACKFILL_START_TIME}"

		# Set the day end time for backfill
		sed -i -e 's/BACKFILL_END_TIME/'"${BACKFILL_END_TIME}"'/g' ${SRC_FOLDER}/properties_backfill.xml
		printInfo "Set backfill BACKFILL_END_TIME to: ${BACKFILL_END_TIME}."
	else
		printInfo "Backfill pipeline time set is skipped."
	fi


	# -------------- (6) Push files to HDFS --------------------------
	printInfo "Pushing the pipeline from ${SRC_FOLDER} to the grid on ${DEST_FOLDER} ..."
	HDFS_PUT_RESULT=`hdfs_folder_put ${SRC_FOLDER} ${DEST_FOLDER}`
	printInfo "Done pushing the pipeline to the grid with result ${HDFS_PUT_RESULT}"

	# Check Result for Push files to HDFS
	if [ "${HDFS_PUT_RESULT}" != 'success' ]; then
		printError "Failed to put the resource files to HDFS: ${HDFS_PUT_RESULT}"
		exit 1
	fi


	# -------------- (7) lanuch the new pipeline ---------------
	printInfo "The new pipeline will be launched with the following times: ${REGULAR_START_TIME}"
	printInfo "   REGULAR_START_TIME: ${REGULAR_START_TIME}"
	printInfo "   BACKFILL_START_TIME: ${BACKFILL_START_TIME}"
	printInfo "   BACKFILL_END_TIME: ${BACKFILL_END_TIME}"

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
	
	
	# ------------- (8) lanuch backfill pipeline ---------------------
	if [ "${BACKFILL_ELIGIBLE}" = 'true' ]; then
		printInfo "The backfill pipeline will be launched with the following times: ${BACKFILL_START_TIME}"

		# Launch the backfill pipeline
		printInfo "Launching the backfill pipeline..."
		BACKFILL_JOB_SUBMIT_RESULT=`oozie_jobSubmit ${SRC_FOLDER}/properties_backfill.xml ${OOZIE_URL}`
		printInfo "Done launching the backfill pipeline with result ${BACKFILL_JOB_SUBMIT_RESULT}"

		# Check Result for Launch the backfill pipeline
		if [[ "${BACKFILL_JOB_SUBMIT_RESULT}" =~ "Error" ]]; then
			printError "Failed to submit the oozie job for backfill: ${BACKFILL_JOB_SUBMIT_RESULT}"
			exit 1
		fi

		# Fetch backfill oozie job id
		printBackfillOozieJobID "${BACKFILL_JOB_SUBMIT_RESULT}"
	else
		printInfo "Backfill pipeline launch is skipped."
	fi


	# -------------- (9) set end time for old jobs -------------------
	# Check if there are active running bundles. If so, use REGULAR_START_TIME as the TIME_BOUNDARY
    if [ -z "${RETURNED_JOBS}" ]; then
    	printInfo "No need to set end time for old bundles."
    else
		printInfo "There is at least one running pipeline, so we will do a continuous launch."

		# Set the endtime for the previous jobs
		printInfo "Setting end time for previous bundles."
		for job_id in ${RETURNED_JOBS}; do
			printInfo "    Setting endtime to ${REGULAR_START_TIME} for previous job ${job_id}..."
			oozie_jobSetEndTime ${job_id} ${REGULAR_START_TIME}
			printInfo "    Done setting endtime to ${REGULAR_START_TIME} for previous job ${job_id}."
		done
		printInfo "Done setting end time for previous bundles."
	fi
	
	
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
	printError "Must accept 7 arguments for datamart: name of the pipleline, its version, its owner, the path of the folder to be uploaded to HDFS, backfill start date, oozie job type, and oozie backfill job type."
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
export BACKFILL_START_PARAM=`echo $5`
export OOZIE_JOB_TYPE=`echo $6`
export OOZIE_BACKFILL_JOB_TYPE=`echo $7`

exportSetting
source common.sh

printInfo "------ PRINT SCRIPT INPUTS ------"
printInfo "Pipeline name is ${PIPELINE_NAME}"
printInfo "Pipeline version is ${PIPELINE_VERSION}"
printInfo "Pipeline owner is ${PIPELINE_OWNER}"
printInfo "Folder path is ${SRC_FOLDER}"
printInfo "Backfill start time parameter is ${BACKFILL_START_PARAM}"
printInfo "Oozie job type is ${OOZIE_JOB_TYPE}"
printInfo "Oozie backfill job type is ${OOZIE_BACKFILL_JOB_TYPE}"
printInfo "------ PRINT ENV VARS ------"
printInfo "HEADLESS_USER=${HEADLESS_USER}"
printInfo "DEPLOY_PATH=${DEPLOY_PATH}"
printInfo "DEST_FOLDER=${DEST_FOLDER}"
printInfo "INSTANCE_NAME=${INSTANCE_NAME}"
printInfo "PRODUCT_NAME=${PRODUCT_NAME}"
printInfo "OOZIE_URL=${OOZIE_URL}"
printInfo "------ DONE PRINTING ------"

exportSetting
deployPipeline
