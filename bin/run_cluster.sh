#!/bin/sh

# re-editable template for starting a bunch of servers based on the config files in a directory

for c in `ls confs`
do
  filename=$(basename "$c" | sed "s/.conf$//") 
  echo $filename
  host=`echo $filename | sed s/\:.*$//`
  scp confs/$filename $host:~/.
  echo "host $host"
done

