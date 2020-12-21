set -e

############## Util ###############

printInfo() {
	echo "[DMART PIPELINE CD][INFO] $1"
}

printError() {
	# stderr
	echo "[DMART PIPELINE CD][ERROR] $1" >&2
}

############## MAIN ENTRY ###############

if [ $# -ne 7 ]; then
	printError "Must accept 7 arguments: name of the pipleline, its version, its owner, the path of the folder to be uploaded to HDFS, backfill start date, oozie job type, and oozie backfill job type."
	exit 1
fi

printInfo "OK"
exit 0