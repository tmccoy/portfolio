#!/bin/bash
# this script takes a users directory and fixes any videos with broken symlinks
cd /net/den2filer25/vol/vol1/albums/g169/thelystras/Seans\ Cellphone/My\ T-Mobile\ Album


for file in $(find . -maxdepth 1 -type l  ! -exec test -r {} \; -print); do 
	vid=$(echo $file | grep m. | awk -F "." '{ print $3, ".", $4 }' | sed -e 's/ //g')
	if [ -z "$vid" ]
	then
		continue
	else
		rm m.$vid
		ln -s $vid m.$vid
		echo "$vid fixed"
	fi

done
