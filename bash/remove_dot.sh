#!/bin/bash
# this script makes hidden files visible
IFS=$'\n'
for i in $(find . -type d -path "*.highres*"); do
	old=$i
	new=$(echo $i | sed -e 's/\.//g')
	echo "old: "$old
	echo "new: ""."$new
	mv $old "."$new
done
