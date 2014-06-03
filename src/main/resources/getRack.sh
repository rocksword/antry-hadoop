#!/bin/bash
ipaddr=$1
echo $ipaddr

segments=`echo $ipaddr|cut --delimiter=. --fields=4`
echo $segments

if [ "$segments" -lt 128 ]; then
 echo /rack-1
else
 echo /rack-2
fi
