#!/bin/bash

function get_date_path()
{
    date=`date -d '2 day ago' +%Y%m%d`
    echo "${date:0:4}/${date:4:2}/${date:0-2}"
}

function zip()
{
    category=$1
    date=$2
    from_path="/data/2/billing_archive"
    to_path="/data/1/billing_archive"

    folder="${from_path}/${category}/${date}"
    target="${to_path}/${category}/${date}"

    echo "start to zip files in folder ${folder}"
    cd ${folder}
    mkdir -p ${target}

    if [ ${category} = "cdn" ]
    then
        filename="${target}/${folder:0-2}.tar.gz"
        echo "zip to ${filename}"
        tar zcf ${filename} *
        rm -r *
    else
        for name in `ls`
        do
            filename="${target}/${name}.tar.gz"
            echo "zip to ${filename}"
            tar zcf ${filename} ${name}
            rm -r ${name}
            sleep 5s
        done
    fi

    echo "done!"
}

function upload()
{
    category=$1
    date=$2
    from_path="/data/1/billing_archive"
    to_path="oss://agora-billing"

    folder="${from_path}/${category}/${date}"
    target="${to_path}/${category}/${date}"

    echo "start to upload files in folder ${folder}"
    cd ${folder}

    for name in `ls`
    do
        filename="${target}/${name}"

        echo "upload to ${filename}"
        aliyuncli oss MultiUpload ${name} ${filename}
        sleep 5s
    done 

    echo "done!"
}

date_path=$(get_date_path)

zip "cdn" "${date_path}"
zip "vos" "${date_path}"

#upload "cdn" "${date_path}"
#upload "vos" "${date_path}"