#!/bin/bash
workdir=$(cd `dirname $0`;pwd)
cd $workdir
 
echo "current work direcory: $workdir"

pid=`cat run/task_poll_thread.pid`
echo $pid

pid_exist=$(pgrep -f task_poll_thread.py | awk '{print $1}' | grep -w $pid)
echo $pid_exist

if [[ "$pid_exist" == "$pid" ]];then
     echo "task_poll_thread.py is running"
else
     echo "task_poll_thread.py is not running"
     python task_poll_thread.py &
fi
