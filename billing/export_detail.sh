#!/bin/bash

# get date to export
function get_date_path()
{
    date=`date -d '1 day ago' +%Y%m%d`
    echo "${date:0:4}${date:4:2}${date:0-2}"
}

# export all billing detail files from hdfs
function export_file()
{
    folder=$1
    
    echo "start to export folder ${folder}"
    src="/tmp/billing/${folder}"
    dest="/data/2/billing_detail"
    hdfs dfs -get ${src} ${dest}

    echo "clear and distribute"
    subfolder="${dest}/${folder}"
    day=${folder:0-4}
    cd ${subfolder}
    rm part-r-*
    rm _SUCCESS
    distribute_file ${folder} ${day} 235-r-00025
    distribute_file ${folder} ${day} 5647-r-00007
    distribute_file ${folder} ${day} 9181-r-00001

    echo "zip file"
    zipfile="${dest}/${folder}.tar.gz"
    tar zcvf ${zipfile} ${subfolder}
    rm -r ${subfolder}
    echo "done!"
}

# distribute file
function distribute_file()
{
    folder=$1
    day=$2
    file=$3

    vid=${file%%-*}
    local src="/data/2/billing_detail/${folder}/${file}"
    local dest="/home/devops/billing_detail/${vid}/${vid}_${day}.csv"

    cp ${src} ${dest}
}

# upload file
function upload_file()
{
    date=$1
    year=${date:0:4}
    month=${date:4:2}
    key="detail/${year}/${month}/${date}.tar.gz"
    filename="/data/2/billing_detail/${date}.tar.gz"
    python upload.py ${key} ${filename}
}


date_path=$(get_date_path)
export_file ${date_path}
upload_file ${date_path}
