#!/bin/bash

freeMem=`free -m | sed -n 2p  | awk '{print $4}'`
if [ $freeMem -gt 300 ]
then 
  echo ">300MB"
else 
  echo "<300MB"
  pm2 restart 0
fi