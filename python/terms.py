# this program takes a set of terms and returns a count of their existence within user metadata from the 58 silo databases
import sys, os
from sys import stdout
import MySQLdb as mdb
import MySQLdb.cursors
import Queue 
import threading
import pdb

# gets the silodb and port map from usermaster
class get_silo_map:
    def __init__(self, verbose, test_mode):
        self.User_Master_Connect = mdb.connect('userreport.int.photobucket.com', 'ro_user', 'n0acc3ss', 'photobucket')
        self.verbose = verbose
        self.test_mode = test_mode

    def connect_to_user_master(self):
        with self.User_Master_Connect:
            cursor = self.User_Master_Connect.cursor()

            if self.test_mode:  							# if test mode is enabled limit to 3 silos
            	ex_cursor = cursor.execute("SELECT silo_db, db_port from silo_map limit 3")
            else:
            	ex_cursor = cursor.execute("SELECT silo_db, db_port from silo_map")

            output = cursor.fetchall()
            output_dict = {}
            for (silo, port) in output:
            	output_dict[(silo)] = port             # create a dictionary key is silodb value is port  

            if self.verbose:
                print output_dict
            
            print "Silo List Loaded"
            cursor.close()
            return output_dict

# class that connects to silo db
class connect_to_silo:
	pics_count = 0
	album_count = 0
	silo_count = 0
	tags_count = 0
	description_count = 0
	title_count = 0

	def __init__(self, silo, port, verbose):
		self.silo = silo
		self.port = port
		self.silo_connect = mdb.connect(host='{0}slave.int.photobucket.com'.format(self.silo), user='ro_user', passwd='n0acc3ss', db='photobucket', port = self.port)
		self.verbose = verbose

		self.get_data()

	def get_data(self):
		# tags, titles, descriptions count
		with self.silo_connect:

			pic_cursor = self.silo_connect.cursor()
			sql = """
					SELECT COUNT(picture.picture_id), COUNT(title), COUNT(tag), COUNT(description)
					FROM picture LEFT OUTER JOIN (picture_tag) on picture.picture_id = picture_tag.picture_id 
					WHERE
				"""
			terms_list = ["pregnant", "baby shower"]

			loop_count = 0
			for term in terms_list:
				if loop_count == 0:
					sql = sql + " " + "LOWER(title) LIKE '%{0}%' OR LOWER(tag) LIKE '%{0}%' OR LOWER(description) LIKE '%{0}%' ".format(term)
				else:
					sql = sql + " " + "OR LOWER(title) LIKE '%{0}%' OR LOWER(tag) LIKE '%{0}%' OR LOWER(description) LIKE '%{0}%' ".format(term)

				loop_count =+ 1

			sql = sql + ";"
			

			pic_cursor.execute(sql)
			pic_output = pic_cursor.fetchall()
			matches = int(pic_output[0][0]) 		# cursor returns a tuple within a tuple this gets the value out
			title_matches = int(pic_output[0][1])
			tag_matches = int(pic_output[0][2])
			description_matches = int(pic_output[0][3])
			lock.acquire()
			# threadsafe vars for tracking counts
			connect_to_silo.pics_count += matches   
			connect_to_silo.title_count += title_matches
			connect_to_silo.tags_count += tag_matches
			connect_to_silo.description_count += description_matches
			lock.release()
			pic_cursor.close()

		# get count of albums 
		with self.silo_connect:
			album_cursor = self.silo_connect.cursor()

			album_sql = """
					SELECT COUNT(picture.picture_id), COUNT(title), COUNT(tag), COUNT(description)
					FROM picture LEFT OUTER JOIN (picture_tag) on picture.picture_id = picture_tag.picture_id 
					WHERE
				"""
			terms_list = ["pregnant", "baby shower"]

			loop_count = 0
			for term in terms_list:
				if loop_count == 0:
					album_sql = album_sql + " " + "LOWER(title) LIKE '%{0}%' OR LOWER(tag) LIKE '%{0}%' OR LOWER(description) LIKE '%{0}%' ".format(term)
				else:
					album_sql = album_sql + " " + "OR LOWER(title) LIKE '%{0}%' OR LOWER(tag) LIKE '%{0}%' OR LOWER(description) LIKE '%{0}%' ".format(term)

				loop_count =+ 1

			album_sql = album_sql + ";"

			album_cursor.execute(album_sql)
			album_output = album_cursor.fetchall()
			album_cursor.close()
			album_matches = int(album_output[0][0])
			lock.acquire()
			connect_to_silo.album_count += album_matches
			connect_to_silo.silo_count += 1
			lock.release()
			album_cursor.close()


#---------------------------BEGIN-------------------------------#


get = get_silo_map(False, True) 				# get current silos and db ports
silo_and_port_dict = get.connect_to_user_master() 

silo_queue = Queue.Queue() 			# build queue for multithreading. Each thread takes a silodb
lock = threading.Lock()


# multithreading functions 
def build_queue(silo_and_port_dict):
	previous = ""
	for silo in silo_and_port_dict:
		if silo == previous:
			continue
		silo_queue.put(silo)
		previous = silo

def worker():
	while True:
		silo = silo_queue.get()
		silo_db_connect = connect_to_silo(silo, int(silo_and_port_dict[silo]), "False") 
		stdout.write("\r%s%s%s%s%s%s" % ( "Silos: {0}".format(connect_to_silo.silo_count)," Albums Matched: {0}".format(connect_to_silo.album_count)," Titles Matched: {0}".format(connect_to_silo.title_count), " Tags Matched: {0}".format(connect_to_silo.tags_count), " Descriptions Matched: {0}".format(connect_to_silo.description_count), " Pics Matched: {0}".format(connect_to_silo.pics_count) ))
		stdout.flush()
		silo_queue.task_done()

		
# start threads
for threads in range (58):					# one for each silo
	silo_thread = threading.Thread(target=worker)
	silo_thread.daemon = True
	silo_thread.start()


lock.acquire()
build_queue(silo_and_port_dict)
lock.release()
silo_queue.join()
pdb.set_trace()
print "\n"
print "finished"