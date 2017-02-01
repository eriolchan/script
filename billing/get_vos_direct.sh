#!/bin/bash

function get_vos_direct()
{
    filename="vos_server_direct"
    serverPort=20220

    echo "get vos log directly"

    while read serverIp
    do
        echo "`date +"%F %T"` info MQLog[$$]: rsync data from $serverIp $serverPort"
        rsync --timeout=300 -avz -e "ssh -oConnectTimeout=10 -oStrictHostKeyChecking=no -p $serverPort -i /home/devops/.ssh/devops.pem" --remove-source-files --exclude=*.tmp devops@$serverIp:/home/devops/mq_video_bill2/ /data/2/billing_logs/vos/
    done < ${filename}

    sleep 60
    get_vos_direct
}

get_vos_direct
