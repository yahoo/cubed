set -e

############## Util ###############

printError() {
	# stderr
	echo "[DMART PIPELINE CD][ERROR] $1" >&2
}

############## MAIN ENTRY ###############

printError "failure"
exit 1