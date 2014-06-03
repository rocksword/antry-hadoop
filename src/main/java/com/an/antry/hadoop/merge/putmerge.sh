#!/bin/bash

#./putmerge.sh /home/hadoop/suss/files_check file:///home/hadoop/suss/merge.txt
set -e

export SUSS_HOME=/home/hadoop/suss

for f in $HADOOP_INSTALL/share/hadoop/common/*.jar; do
  export CLASSPATH=$CLASSPATH:$f
done

for f in $HADOOP_INSTALL/share/hadoop/common/lib/*.jar; do
  export CLASSPATH=$CLASSPATH:$f
done

for f in $HADOOP_INSTALL/share/hadoop/hdfs/lib/*.jar; do
  export CLASSPATH=$CLASSPATH:$f
done

CLASSPATH=$CLASSPATH:$SUSS_HOME/suss-analyzer-0.0.1.jar

java -cp $CLASSPATH com.dell.sonicwall.suss.analyzer.PutMerge $1 $2 $3

exit 0