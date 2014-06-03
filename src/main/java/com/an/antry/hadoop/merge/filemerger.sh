#!/bin/bash

#./merger.sh /home/hadoop/suss/files_check /home/hadoop/suss 2014-05-20
set -e

export SUSS_HOME=/home/hadoop/suss

java -jar $SUSS_HOME/suss-filemerger-0.0.1.jar $1 $2 $3 >$SUSS_HOME/filemerger.err 2>&1 &

exit 0