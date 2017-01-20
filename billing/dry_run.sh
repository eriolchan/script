#!/bin/bash

time=$1

for i in $(seq -f "%02g" 0 23)
do
    echo "process ${i}"
    nohup java -jar ImportLogService-1.0-SNAPSHOT.jar -t cdn -s ${time}:${i} -e ${time}:${i} 2>&1 &

    while [ `ps aux | grep ImportLogService | wc -l` -ne 1 ]
    do
        echo "sleep:${i}"
        sleep 15
    done
done

echo "done!"
