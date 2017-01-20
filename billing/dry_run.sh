#!/bin/bash

time=$1

for i in $(seq -f "%02g" 0 23)
do
    echo "process ${i}"
    nohup java -jar ImportLogService-1.0-SNAPSHOT.jar -t cdn -s ${time}:${i} -e ${time}:${i} 2>&1 &

    number=`ps aux | grep ImportLogService | wc -l`

    while [ ${number} -ne 1 ]
    do
        echo "ps num:${number}, sleep:${i}"
        sleep 10
        number=`ps aux | grep ImportLogService | wc -l`
    done
done

echo "done!"
