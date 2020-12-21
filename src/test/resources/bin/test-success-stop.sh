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

if [ $# -ne 2 ]; then
	printError "Must accept one argument: name of the pipleline and its owner."
	exit 1
fi

printInfo "OK"
exit 0