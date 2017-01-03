#!/bin/bash

function get_cdn_logs()
{
    echo "get all servers from resource db..."
    #for serverIp serverPort in
    while read serverIp serverPort
    do
        echo "`date +"%F %T"` info MQLog[$$]: rsync data from $serverIp $serverPort"
        rsync --timeout=300 -avz -e "ssh -p 22 -oStrictHostKeyChecking=no -i /home/devops/.ssh/devops.pem -A devops@10.1.1.50 ssh -oConnectTimeout=10 -oStrictHostKeyChecking=no -p $serverPort -i /home/devops/.ssh/devops.pem" --remove-source-files --exclude=*.tmp devops@$serverIp:/home/devops/cdn/ /data/billing_logs/cdn/
    done < <(mysql -h10.1.1.51 -P3315 -utest -ppassword agora_resource -N -e "select server,port from servers where cdn = 1 and unix_timestamp(down_time)>`date +'%s'`" | cat);

    sleep 300s
    get_cdn_logs
}

get_cdn_logs
