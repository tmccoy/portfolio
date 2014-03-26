#!/bin/bash
# this script symlinks a users .highres directory to a visible directory allowing their highres images to be downloaded through ftp
UserName=$1

# get user directory
cd ~frog/scripts
dir=$(sh finduserdir $UserName)

echo "Working...."
for album in $(find $dir -type d) 
do
	if [[ "$album" =~ /*_highres ]] ; then # bail if user has albums with _highres 
 		echo "Aborting user has albums containing _highres"
		exit 0
	fi
done

IFS=$'\n' # only split on new line prevents file names with spaces from getting split
for album in $(find $dir -type d)
do
	if [[ "$album" =~ \.+ ]] ; 
		then echo "skipped: $album" # skip . files
	else
		current_album=$(echo "$album" | awk -F "/" '{print $NF}') # get current album name
		sudo -u nobody ln -s $album/.highres/ $album/${current_album}_highres
		echo "linked: $album/.highres/ $album/${current_album}_highres"		
	fi
done 
