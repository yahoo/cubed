#! /bin/bash

OOZIE=$(OOZIE)

function printInfo {
    echo "[DMART PIPELINE CD][INFO] $1"
}

function printError {
    # stderr
    echo "[DMART PIPELINE CD][ERROR] $1" >&2
}

function printOozieJobID {
    echo "[[ID]]$1"
}

function printBackfillOozieJobID {
    echo "[[BACKFILLID]]$1"
}

# --------------------------------- OOZIE --------------------------------- #

function  oozie_jobSearch {
  JOBTYPE=$1
  JOB_NAME_PATTERN=$2
  OOZIE_URL=$3

  # check inputs
  if [[ ! "$JOBTYPE" =~ "wf" ]] && [[ ! "$JOBTYPE" =~ "coordinator" ]] && [[ ! "$JOBTYPE" =~ "bundle" ]]; then
    echo "ERROR: jobSearch requires jobtype of: wf, coordinator or bundle, got jobtype: $JOBTYPE"
    exit 1005
  fi

  if [ -z "$JOB_NAME_PATTERN" ]; then
    echo "ERROR: no job name pattern provided"
  fi

  JOB_IDS=`$OOZIE jobs -oozie $OOZIE_URL -jobtype $JOBTYPE -filter "name=$JOB_NAME_PATTERN;status=SUCCEEDED;status=RUNNING;status=RUNNINGWITHERROR" | grep $JOB_NAME_PATTERN | awk '{print $1}' `

  echo $JOB_IDS
}

function oozie_jobKill {
  JOBID=$1
  OOZIE_URL=$2

  RESULT=`$OOZIE job -oozie $OOZIE_URL -kill $JOBID`

  echo $RESULT
}

function oozie_jobSubmit {
  USER_JOB=$1
  OOZIE_URL=$2

  if [ ! -f "$USER_JOB" ]; then
    echo "ERROR: $USER_JOB is not a valid file"
    return 1
  fi

  RESULT=`$OOZIE job -oozie $OOZIE_URL -run -config $USER_JOB`

  # get the job's ID
  OOZIEJOBID=`echo $RESULT | awk -F'job: ' '{print $NF}'`
  echo $OOZIEJOBID
}

function oozie_jobSetEndTime {
  JOBID=$1
  ENDTIME=$2

  RESULT=`$OOZIE job -oozie $OOZIE_URL -change $JOBID -value endtime=$ENDTIME`

  echo $RESULT
}

# --------------------------------- HDFS --------------------------------- #

function hdfs_ls {
    local DIR=$1

    hdfs dfs -ls $DIR >/dev/null 2>&1

    if [ $? -eq 0 ]; then
      echo "found on hdfs"
    fi
}

# Put a folder onto hdfs
# e.g. LOCALSRC = /a/b/c, HDFSDEST=x/y/z
# result = x/y/z/c
function hdfs_folder_put {

  local LOCALSRC=$1
  local HDFSDEST=$2

  RESULT_PUT=`hdfs dfs -put $LOCALSRC $HDFSDEST`
  if [ $? -ne 0 ]; then
      echo "ERROR: put failed, result is: $RESULT_PUT"
      return 1
  else
      echo "success"
      return 0
  fi
}
