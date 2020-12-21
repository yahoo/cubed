set -e

############## Util ###############

function printError {
	# stderr
	echo "[DMART PIPELINE CD][ERROR] $1" >&2
}

############## MAIN ENTRY ###############

printError "failure"
exit 1