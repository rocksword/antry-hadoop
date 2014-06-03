#!/bin/bash
set -e

export SUSS_DATE=2014-05-20
export SUSS_HOME=/home/hadoop/suss

cd $SUSS_HOME

#run map-reduce jobs
HADOOP_CLASSPATH=`${HBASE_HOME}/bin/hbase classpath`
hadoop fs -rm -r /sonicwall/output/susscheck
$HADOOP_HOME/bin/hadoop jar suss-analyzer-0.0.1.jar com.dell.sonicwall.suss.analyzer.WholeFileTest /sonicwall/input/susscheck /sonicwall/output/susscheck
#clean