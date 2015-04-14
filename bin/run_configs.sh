#!/bin/sh

# re-editable template for starting a bunch of servers based on the config files in a directory

for c in `ls *.conf`
do
  filename=$(basename "$c" | sed "s/.conf$//") 
  echo $filename
done

