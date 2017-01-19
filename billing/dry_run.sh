#!/bin/bash

time=$1

for i in $(seq -f "%02g" 0 23)
do
    nohup java -jar ImportLogService-1.0-SNAPSHOT.jar -t vos -s ${time}:${i} -e ${time}:${i} 2>&1 &

    number=`ps aux | grep ImportLogService | wc -l`

    while [ ${number} -ne 1 ]
    do 
        sleep 60
    done
done

echo "done!"
