set +e

function exportSetting {
	export HEADLESS_USER=$(USER_NAME)
	export INSTANCE_NAME=$(INSTANCE_NAME)
	export PRODUCT_NAME="${INSTANCE_NAME}_${PIPELINE_NAME}"
	export OOZIE_URL=$(OOZIE_URL)
}

function stopPipeline {

	# -------------- (1) search for existing (old) jobs -------------------
	# Get all running or waiting jobs matching the product name/owner combination
	printInfo "Searching for jobs named ${PRODUCT_NAME}_${PIPELINE_OWNER}..."
	RETURNED_JOBS=`oozie_jobSearch bundle ${PRODUCT_NAME}_${PIPELINE_OWNER} ${OOZIE_URL}`
	printInfo "Searching result of returned jobs: ${RETURNED_JOBS}"

	# Get previous job ids, and store in an array
	if [ -z "${RETURNED_JOBS}" ]; then
		printInfo "Warning: failed to find existing jobs for ${PRODUCT_NAME}."
		return 0
	fi

	# -------------- (2) kill existing jobs ---------------
	printInfo "Killing existing bundles."
	for job_id in ${RETURNED_JOBS}; do
		oozie_jobKill ${job_id} ${OOZIE_URL}
	done
	printInfo "Done killing existing bundles."

	# -----------------------------------------------------------------------

	# Done
	printInfo "Done stopping the pipeline."
}

function stopBackfillPipeline {

	# -------------- (1) search for existing backfill jobs -------------------
	# Get all running or waiting jobs matching the product name/backfill/owner combination
	printInfo "Searching for backfill jobs named ${PRODUCT_NAME}_backfill_${PIPELINE_OWNER}..."
	RETURNED_JOBS=`oozie_jobSearch bundle ${PRODUCT_NAME}_backfill_${PIPELINE_OWNER} ${OOZIE_URL}`
	printInfo "Searching result of returned backfill jobs: ${RETURNED_JOBS}"

	# Get previous job ids, and store in an array
	if [ -z "${RETURNED_JOBS}" ]; then
		printInfo "Warning: failed to find existing backfill jobs for ${PRODUCT_NAME}."
		return '0'
	fi

	# -------------- (2) kill existing jobs ---------------
	printInfo "Killing existing backfill bundles."
	for job_id in ${RETURNED_JOBS}; do
		oozie_jobKill ${job_id} ${OOZIE_URL}
	done
	printInfo "Done killing existing backfill bundles."

	# -----------------------------------------------------------------------

	# Done
	printInfo "Done stopping the backfill pipeline."
}

############## MAIN ENTRY ###############

if [ $# -ne 2 ]; then
	printError "Must accept two argument: name of the pipeline and its owner"
	exit 1
fi

export PIPELINE_NAME=`echo $1`
export PIPELINE_OWNER=`echo $2`

exportSetting
source common.sh

printInfo "------ PRINT SCRIPT INPUTS ------"
printInfo "Pipeline name is ${PIPELINE_NAME}"
printInfo "Pipeline owner is ${PIPELINE_OWNER}"
printInfo "------ PRINT ENV VARS ------"
printInfo "HEADLESS_USER=${HEADLESS_USER}"
printInfo "INSTANCE_NAME=${INSTANCE_NAME}"
printInfo "PRODUCT_NAME=${PRODUCT_NAME}"
printInfo "OOZIE_URL=${OOZIE_URL}"
printInfo "------ DONE PRINT ENV VARS ------"

stopPipeline
stopBackfillPipeline
exit 0
