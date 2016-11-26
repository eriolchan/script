#!/bin/bash

while true
do
    current_time=`date`
    result=`curl -o /dev/null --connect-time 30 -s "http://www.agora.io/cn/" -w "result: %{http_code}, time: %{time_total}s\n"`
    echo $current_time, $result
    sleep 30
done