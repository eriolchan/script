#!/bin/bash

for i in 'c' 'd' 'e' 'f' 'g' 'h' 'i' 'j' 'k'
do
  p="sd${i}"
  echo ${p}
  parted -s /dev/${p} mklabel gpt
  parted /dev/${p} mkpart primary 0% 100%
  p1="${p}1"
  mkfs.ext4 /dev/${p1}
done
