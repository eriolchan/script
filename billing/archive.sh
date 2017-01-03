#!/bin/bash

function archive()
{
    folder=$1
    filename="${folder:0-2}.tar.gz"

    echo "begin to archive $folder"

    cd $folder
    tar zcf ../$filename *
    rm -r *
    mv ../$filename .
}

function get_date_path()
{
    date=`date -d '2 day ago' +%Y%m%d`
    echo "${date:0:4}/${date:4:2}/${date:0-2}"
}


basePath="/data/3/billing_archive"
datePath=$(get_date_path)

cdnPath="${basePath}/cdn/${datePath}"
vosPath="${basePath}/vos/${datePath}"

archive $cdnPath
archive $vosPath

echo "done!"
