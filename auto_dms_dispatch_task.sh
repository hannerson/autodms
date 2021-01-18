#!/bin/bash
workdir=$(cd `dirname $0`;pwd)
cd $workdir
 
echo "current work direcory: $workdir"

pid=`cat run/dispatcher_task.pid`
echo $pid

pid_exist=$(pgrep -f dispatcher_task.py | awk '{print $1}' | grep -w $pid)
echo $pid_exist

if [[ "$pid_exist" == "$pid" ]];then
     echo "dispatcher_task.py is running"
else
     echo "dispatcher_task.py is not running"
     python dispatcher_task.py &
fi
