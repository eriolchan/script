#!/bin/sh

remove()
{
    file=$1
    HADOOP_USER_NAME=hdfs hdfs dfs -rm -r ${file}
}

for i in $(seq -f "%02g" 8 31)
do
    file="/tmp/export/all_03${i}"
    echo "remove file ${file}"
    remove ${file}
done

echo "done!"
