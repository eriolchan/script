#/bin/bash

function archive()
{
    folder=$1
    target=${folder/2/1}
    filename="${target}/${folder:0-2}.tar.gz"

    echo "begin to archive ${folder}"

    cd $folder
    mkdir -p ${target}
    tar zcf ${filename} *
    rm -r *
}

function get_date_path()
{
    date=`date -d '2 day ago' +%Y%m%d`
    echo "${date:0:4}/${date:4:2}/${date:0-2}"
}


basePath="/data/2/billing_archive"
datePath=$(get_date_path)

cdnPath="${basePath}/cdn/${datePath}"
vosPath="${basePath}/vos/${datePath}"

archive $cdnPath
archive $vosPath

echo "done!"
