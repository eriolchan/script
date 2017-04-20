#!/bin/bash

function get_date_path()
{
    date=`date -d '1 day ago' +%Y%m%d`
    echo "${date:0:4}${date:4:2}${date:0-2}"
}

function hdfs_get()
{
    folder=$1
    day=$2
    file=$3

    src="/tmp/billing/${folder}/${file}"
    vid=${file%%-*}
    dest="/home/devops/billing_detail/${vid}/${vid}_${day}.csv"

    hdfs dfs -get ${src} ${dest}
}

function export_file()
{
    folder=$1
    day=${folder:0-4}
    base=""

    echo "start to get folder ${base}/${folder}"
    hdfs_get ${folder} ${day} 235-r-00025
    hdfs_get ${folder} ${day} 5647-r-00007
    hdfs_get ${folder} ${day} 9181-r-00001
    hdfs_get ${folder} ${day} 16191-r-00021
    
    echo "done!"
}


date_path=$(get_date_path)
export_file ${date_path}
