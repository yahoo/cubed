#!/bin/bash

function printInfo {
        echo "[" `date` "][INFO]" \$1
}

function printError {
        echo "[" `date` "][ERROR]" \$1
}

function druidLoad {
    MAX_SLEEP_MINUTES=\$1
    INDEX_JSON=\$2

    # Trigger the Druid job
    DRUID_JSON=`curl -L \
                     -X 'POST' \
                     -H 'Content-Type:application/json' \
                     --proxy "$DRUID_HTTP_PROXY$" \
                     -d "\$INDEX_JSON" "http://$DRUID_INDEX$/druid/indexer/v1/task"`

    # Print the result from Druid
    printInfo "Got result from Druid: \$DRUID_JSON"

    # Extract task id
    TASK_ID=`echo \$DRUID_JSON | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["task"])'`
    printInfo "Got task id: \$TASK_ID"

    # Check status of task every minute for \$MAX_SLEEP_MINUTES minutes
    for ((i=1;i<=\$MAX_SLEEP_MINUTES;i++)); do
            # Get the job status
            TASK_JSON=`curl -L \
                            -X 'GET' \
                            -H 'Content-Type:application/json' \
                            --proxy "$DRUID_HTTP_PROXY$" \
                            -d "\$INDEX_JSON" "http://$DRUID_INDEX$/druid/indexer/v1/task/\${TASK_ID}/status"`
            printInfo "Got task json: \${TASK_JSON}"
            # Get the task status
            TASK_STATUS=`echo \$TASK_JSON | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["status"]["status"])'`
            printInfo "Got the task status: \$TASK_STATUS"
            if [ "\$TASK_STATUS" == "SUCCESS" ]; then
                    printInfo "Task completed!"
                    return 1
            fi
            if [ "\$TASK_STATUS" == "FAILED" ]; then
                    printError "Task failed!"
                    return 2
            fi
            if [ "\$TASK_STATUS" == "" ]; then
                    printError "Task failed!"
                    return 2
            fi
            printInfo "Task status not SUCCESS, sleeping for 1 minute: (\$i/\$MAX_SLEEP_MINUTES minutes)"
            sleep 1m
    done
    return 2
}

GRANULARITY="$GRANULARITY_PARAMETER$"
MAX_NUM_DRUID_LOAD_RETRIES=3

# Check that we got 5 parameters for hour
if [[ "\$GRANULARITY" = "hour" ]]; then
    if [[ \$# -lt 5 ]]; then
        printError "Must provide YEAR, MONTH, DAY, HOUR, and MAX_SLEEP_MINUTES parameters."
        exit 1
    fi
fi

# Check that we got 4 parameters for day
if [[ "\$GRANULARITY" = "day" ]]; then
    if [[ \$# -lt 4 ]]; then
        printError "Must provide YEAR, MONTH, DAY, and MAX_SLEEP_MINUTES parameters."
        exit 1
    fi
fi

# Load index json data
INDEX_JSON=`cat index.json`

# Get the date parameters
YEAR=\$1
MONTH=\$2
DAY=\$3
if [[ "\$GRANULARITY" = "hour" ]]; then
    HOUR=\$4
    MAX_SLEEP_MINUTES=\$5
fi
if [[ "\$GRANULARITY" = "day" ]]; then
    MAX_SLEEP_MINUTES=\$4
fi

# Replace dates in the JSON
INDEX_JSON=\${INDEX_JSON//YEAR/\$YEAR}
INDEX_JSON=\${INDEX_JSON//MONTH/\$MONTH}
if [[ "\$GRANULARITY" = "hour" ]]; then
    INDEX_JSON=\${INDEX_JSON//DAY/\$DAY}
    INDEX_JSON=\${INDEX_JSON//HOUR/\$HOUR}
fi
if [[ "\$GRANULARITY" = "day" ]]; then
    INDEX_JSON=\${INDEX_JSON//DAY/\$DAY}
fi

# Update user on status
printInfo "Submitting index job to Druid using following JSON: \$INDEX_JSON"

RETURN_STATUS=2
# Try to load into Druid and retry MAX_NUM_DRUID_LOAD_RETRIES times if it fails. 
# If RETURN_STATUS is 1, it was successful. If it is 2, it was unsuccessful. 
for ((c=1;c<=\$MAX_NUM_DRUID_LOAD_RETRIES;c++)); do
    if [[ \$RETURN_STATUS -eq 2 ]]; then
        printInfo "Submitting index job to Druid, attempt \$c out of \$MAX_NUM_DRUID_LOAD_RETRIES"
        druidLoad \$MAX_SLEEP_MINUTES "\$INDEX_JSON"
        RETURN_STATUS=\$?
    fi
    if [[ \$RETURN_STATUS -eq 1 ]]; then
        printError "Task completed, exiting"
        exit 0
    fi
    sleep 15m
done


# Not SUCCESS, fail job
printError "Task did not complete with state SUCCESS after \$MAX_NUM_DRUID_LOAD_RETRIES attempts, marking job as failed."
exit 1
