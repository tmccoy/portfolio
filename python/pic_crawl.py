#!/usr/bin/python
# this program crawls a list of users filesystems and returns the # with exif data. 
import sys, os
from sys import stdout
import MySQLdb as mdb
import MySQLdb.cursors
import json
from PIL import Image
from PIL.ExifTags import TAGS
import Queue 
import threading

# load album map. A modified album.map file. Imports filer locations using the vee as a key. 
album_dict = {}
with open('album_map', 'r') as album_map:
    for line in album_map:
        (key, value) = line.split()
        album_dict[(key)] = value

class get_album_location:
    def __init__(self, user_id, album_dict, verbose):
        self.user_id = user_id
        self.album_dict = album_dict
        self.User_Master_Connect = mdb.connect('userreport.int.photobucket.com', 'ro_user', 'n0acc3ss', 'photobucket', cursorclass=MySQLdb.cursors.DictCursor)
        self.verbose = verbose
        self.directory = ""
        self.username = ""

        self.connect_to_user_master()


    def connect_to_user_master(self):
        with self.User_Master_Connect:
            cursor = self.User_Master_Connect.cursor()
            pt_cursor = cursor.execute("SELECT user_id, username, email, silo_db, subdomain, directory, active, g.creation_dt FROM global_user g, silo_map s WHERE s.silo_id=g.silo and user_id={0}".format(self.user_id))
            output = cursor.fetchall()
            output_dict = output[0]   # cursor returns a dict in a tuple, this pulls the dict out
            self.username = output_dict['username']
            self.directory = output_dict['directory']

            if self.verbose:
                print "User Dict:"
                print output_dict
                print "Username: {0}".format(self.username)
                print "Directory: {0}".format(self.directory)

        self.find_album()

    def find_album(self):
        filer_dir = album_dict[self.directory]
        user_directory = filer_dir + '/' + self.username    
        return user_directory

        if self.verbose:
            print "Filer Path: " + filer_dir
            print "User Directory: " + user_directory
            print "User Directory Found: " + user_directory   

class fs_crawl:

    total_pic_count = 0
    exif_count = 0

    def __init__(self, user_directory):
        self.user_directory = user_directory
        self.get_file_list()

    def get_file_list(self):
        self.pic_list = []    
        for root, dirs, files in os.walk("{0}".format(self.user_directory)):
            for filename in files:
                if not filename.startswith('th'):
                    fullpath = os.path.join(root,filename)
                    if not ".highres" in fullpath and not ".alt" in fullpath and not ".profile" in fullpath and not ".data" in fullpath and ".jpg" in fullpath:
                        pic = fullpath
                        self.pic_list.append(pic)
        
        self.get_exif_data()

    def get_exif_data(self):
        exif_dict = {}

        for pic_location in self.pic_list:
            try:
                pic = Image.open(pic_location)
                lock.acquire()
                fs_crawl.total_pic_count +=1
                lock.release()
            except IOError:
                break
            try:
                for (k, v) in pic._getexif().iteritems():
                    exif_dict[(TAGS.get(k))] = v                  
                lock.acquire()
                fs_crawl.exif_count += 1
                lock.release()
                print exif_dict
            except NameError:
                continue
            except AttributeError:
                continue 
            except KeyError:
                continue          

user_queue = Queue.Queue(0)
user_albums_list = []

def build_queue(user_albums_list):
    previous = ""
    for user in user_albums_list:
        if user == previous:
            continue
        user_queue.put(user)
        previous = user
    queue_size = user_queue.qsize()
    print "Queue Size: {0}".format(queue_size)

def worker():
    while True:
        album_location = user_queue.get()
        crawl = fs_crawl(user_location)
        user_queue.task_done()
        stdout.write("\r%s%s" % ( "Pictures Processed: {0} | ".format(fs_crawl.total_pic_count), "With Exif: {0}".format(fs_crawl.exif_count) ) )
        stdout.flush()

#-----------------------------------
lock = threading.Lock()
with open('users', 'r') as user_list:
    for users, line in enumerate(user_list):
        get = get_album_location(line, album_dict, False)
        user_location = get.find_album()
        user_albums_list.append(user_location)
        stdout.write("\r%s" % ( "Users Loaded: {0}".format(users) ))
        stdout.flush()

print "\n"


for threads in range(4):
    user_thread = threading.Thread(target=worker)
    user_thread.daemon = True
    user_thread.start()

lock.acquire()
build_queue(user_albums_list)
lock.release()

user_queue.join()
print "\n"