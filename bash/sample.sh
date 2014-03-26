#!/bin/bash

# grabs a random 9 photos from users dir and outputs a working link. 

while read dir; do
cd $dir
vee=$(echo "$dir" | awk -F "/" '{print $7}')
username=$(echo "$dir" | awk -F "/" '{print $8}')

#find . -maxdepth 1 -not -type d -and -not -name '.*'-and -not -name 'th_*' | awk '{ print substr($1,2); }' | head -n 9 |
find . -maxdepth 1 -not -type d -and -not -name '.*' -and -not -name 'th_*' | awk '{ print substr($1,2); }' | sort -R | tail -9 | 
	while read pic ; do echo "www.photobucket.com/albums/$vee/$username$pic" 
	done
done < sample_dirs.txt

