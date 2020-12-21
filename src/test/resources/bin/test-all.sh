set -e

############## Util ###############

function printInfo {
	echo "[DMART PIPELINE CD][INFO] $1"
}

function printError {
	# stderr
	echo "[DMART PIPELINE CD][ERROR] $1" >&2
}

############## MAIN ENTRY ###############

printInfo "OK"
printError "ERROR"
exit 0