#!/usr/bin/python
# this program returns all relevant user information from the database(s)
import sys
from sys import stdout
import MySQLdb as mdb
import MySQLdb.cursors
from prettytable import PrettyTable
from optparse import OptionParser
import pdb

usage = "usage: ./rpt_user.py  (-i [user_id] || -u [username] || -e [email] ) [-c for cmod report] [-a for all available tables]"
parser = OptionParser(usage)
parser.add_option("-i", action="store", dest="user_id", help="Run report using user_id")
parser.add_option("-u", action="store", dest="username", help="Run report using username")
parser.add_option("-e", action="store", dest="email", help="Run report using email")
parser.add_option("-c", action="store_true", dest="cmod", help="Run content mod report (still requires -i, -u or -e)")
parser.add_option("-a", action="store_true", dest="all", help="Run the report with all available tables")
(options, args) = parser.parse_args()

def get_user_id(field, value):
    User_Master_Connect = mdb.connect('usermaster.int.photobucket.com', 'ro_user', 'n0acc3ss', 'photobucket')
    with User_Master_Connect:
        cursor = User_Master_Connect.cursor()
        cursor.execute("SELECT user_id FROM global_user where {field} = '{value}' ;".format(field=field, value=value))
        output = cursor.fetchall()
        try:
            user_id = output[0][0]
            return user_id
        except IndexError:
            try:
                cursor.execute("SELECT user_id FROM delete_global_user where {field} = '{value}' ;".format(field=field, value=value))
                output = cursor.fetchall()
                user_id = output[0][0]
                return user_id
            except IndexError:
                print "User not found"
                print usage
                sys.exit()


class user_report(object):
    def __init__(self, user_id, check_for_dup):
        self.user_id = user_id
        self.silodb = ""
        self.port = ""
        self.username = ""
        self.email = ""
        self.check_for_dup = check_for_dup

    def __get_silo_port_map__(self):
         self.User_Master_Connect = mdb.connect('userreport.int.photobucket.com', 'ro_user', 'n0acc3ss', 'photobucket')
         with self.User_Master_Connect:
            cursor = self.User_Master_Connect.cursor()
            cursor.execute("SELECT silo_db, db_port from silo_map")
            output = cursor.fetchall()
            silo_port_dict = {}
            for (silo, port) in output:
                silo_port_dict[(silo)] = port

            return silo_port_dict



    def get_report(self):
        self.__connect_to_global__()
        self.__get_global_user_info__()
        self.__get_delete_info__()
        if self.check_for_dup:
            self.__check_for_dup_delete_info_username__()
            self.__check_for_dup_delete_info_email__()
        self.__get_user_delete_queue__()
        self.__get_delete_comment__()
        self.__kill_global_connection__()

        self.__connect_to_silo__()
        self.__get_silo_user_info__()
        self.__get_silo_delete_user_info__()
        self.__get_silo_user_stats__()
        self.__get_silo_user_preference__()
        self.__get_silo_album_preference__()
        self.__kill_silo_connection__()

    def get_full_report(self):
        self.__connect_to_global__()
        self.__get_global_user_info__()
        self.__get_delete_info__()
        if self.check_for_dup:
            self.__check_for_dup_delete_info_username__()
            self.__check_for_dup_delete_info_email__()
        self.__get_global_user_audit__()
        self.__get_user_delete_queue__()
        self.__get_payment_info__()
        self.__get_plus_subscription__()
        self.__get_delete_payment__()
        self.__get_comment__()
        self.__get_delete_comment__()
        self.__get_global_group__()
        self.__kill_global_connection__()

        self.__connect_to_silo__()
        self.__get_silo_user_info__()
        self.__get_silo_user_info_audit__()
        self.__get_silo_delete_user_info__()
        self.__get_silo_story__()
        self.__get_silo_password_audit__()
        self.__get_silo_user_stats__()
        self.__get_silo_user_preference__()
        self.__get_silo_description__()
        self.__get_silo_user_activity___()
        self.__get_silo_new_mobile__()
        self.__get_silo_album_preference__()
        self.__kill_silo_connection__()

    def get_cmod_report(self):
        self.__connect_to_global__()
        self.__get_global_user_info__()

        self.__connect_to_cmod__()
        self.__get_cmod_user_reviews__()
        self.__get_cmod_media_feed__()
        self.__get_cmod_approvals__()
        self.__get_cmod_approvals2__()
        self.__get_cmod_tosses__()
        self.__kill_cmod_connection__()

    def __connect_to_cmod__(self):
        self.cmod_connect = mdb.connect('cmodmaster.int.photobucket.com', 'ro_user', 'n0acc3ss', 'photobucket')
        self.cmod_cursor = self.cmod_connect.cursor()

    def __kill_cmod_connection__(self):
        self.cmod_cursor.close()

    def __connect_to_global__(self):
        self.User_Master_Connect = mdb.connect('usermaster.int.photobucket.com', 'ro_user', 'n0acc3ss', 'photobucket')
        self.global_cursor = self.User_Master_Connect.cursor()

    def __kill_global_connection__(self):
        self.global_cursor.close()

    def __connect_to_silo__(self):
        port_map = self.__get_silo_port_map__()
        self.port = int(port_map[self.silodb])
        self.Silo_Connect = mdb.connect('{0}slave.int.photobucket.com'.format(self.silodb), 'ro_user', 'n0acc3ss', 'photobucket', port=self.port)
        self.silo_cursor = self.Silo_Connect.cursor()

    def __kill_silo_connection__(self):
        self.silo_cursor.close()

    def __make_pretty_and_print__(self, table, fields, output):
        try:
            if output:
                pretty_table = PrettyTable(fields)  
                for i in output:
                    pretty_table.add_row(i)
                print table + ": "
                print pretty_table
                print "\n"
        except IndexError:
            print "\n"

    def __get_cmod_user_reviews__(self):
        with self.cmod_connect:
            self.cmod_cursor.execute("SELECT source,current_state,priority,last_modified FROM user_reviews WHERE user_id={0}".format(self.user_id))
            output=self.cmod_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.cmod_cursor.description]
                self.__make_pretty_and_print__("User Reviews", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                raise

    def __get_cmod_media_feed__(self):
        with self.cmod_connect:
            self.cmod_cursor.execute("SELECT data_source_name, url, process_key, creation_date FROM media_feed f, data_source s WHERE s.data_source_id=f.data_source_id AND user_id={0}".format(self.user_id))
            output=self.cmod_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.cmod_cursor.description]
                self.__make_pretty_and_print__("Media Feed", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                raise

    def __get_cmod_approvals__(self):
        with self.cmod_connect:
            self.cmod_cursor.execute("SELECT media_review_id,source, approval_date, url,username FROM image_approvals t, moderators m WHERE moderator_id=m.id AND user_id={0}".format(self.user_id))
            output=self.cmod_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.cmod_cursor.description]
                self.__make_pretty_and_print__("Approvals", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                raise

    def __get_cmod_approvals2__(self):
        with self.cmod_connect:
            self.cmod_cursor.execute("SELECT media_review_id,approval_date, path,username FROM video_approvals t, moderators m WHERE moderator_id=m.id AND user_id={0}".format(self.user_id))
            output=self.cmod_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.cmod_cursor.description]
                self.__make_pretty_and_print__("Approvals2", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                raise

    def __get_cmod_tosses__(self):
        with self.cmod_connect:
            self.cmod_cursor.execute("SELECT media_review_id,source,media_type,current_state,creation_date,  url,username FROM tossed_media t, moderators m WHERE moderator_id=m.id AND user_id={0}".format(self.user_id))
            output=self.cmod_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.cmod_cursor.description]
                self.__make_pretty_and_print__("Tosses", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                raise
    
    def __get_silo_user_info__(self):
        with self.Silo_Connect:
            self.silo_cursor.execute("SELECT username, birth_date, sex, user_type_id, status, modification_dt, password_hash FROM user_info WHERE user_id={0}".format(self.user_id))
            output = self.silo_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.silo_cursor.description]
                self.__make_pretty_and_print__("User Info", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __get_silo_album_preference__(self):
        with self.Silo_Connect:
            self.silo_cursor.execute("SELECT user_id,album_preference_id,parent_ap_id,is_public,page_views,dir_name ,location, modification_dt FROM album_preference WHERE user_id={0}".format(self.user_id))
            output = self.silo_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.silo_cursor.description]
                self.__make_pretty_and_print__("Album Preference", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise


    def __get_silo_user_info_audit__(self):
        with self.Silo_Connect:
            self.silo_cursor.execute("SELECT password_hash, user FROM user_info_audit WHERE user_id={0}".format(self.user_id))
            output = self.silo_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.silo_cursor.description]
                self.__make_pretty_and_print__("User Info Audit", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __get_silo_delete_user_info__(self):
        with self.Silo_Connect:
            self.silo_cursor.execute("SELECT username, birth_date, sex, user_type_id, status, creation_dt, password_hash FROM delete_user_info WHERE user_id={0}".format(self.user_id))
            output = self.silo_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.silo_cursor.description]
                self.__make_pretty_and_print__("User Delete Info", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise


    def __get_silo_story__(self):
        with self.Silo_Connect:
            self.silo_cursor.execute("SELECT * FROM story WHERE user_id={0}".format(self.user_id))
            output = self.silo_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.silo_cursor.description]
                self.__make_pretty_and_print__("Story", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __get_silo_password_audit__(self):
        with self.Silo_Connect:
            self.silo_cursor.execute("SELECT c.* FROM password_change c WHERE user_id={0}".format(self.user_id))
            output = self.silo_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.silo_cursor.description]
                self.__make_pretty_and_print__("Password Audit", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __get_silo_user_stats__(self):
        with self.Silo_Connect:
            self.silo_cursor.execute("SELECT media_count, media_bytes_stored, storage_limit, storage_reward,join_date, last_accessed_dt, join_ip, last_accessed_ip, tossed_count, join_scid, modification_dt FROM user_stats WHERE user_id={0}".format(self.user_id))
            output = self.silo_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.silo_cursor.description]
                self.__make_pretty_and_print__("User Stats", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __get_silo_user_preference__(self):
        with self.Silo_Connect:
            self.silo_cursor.execute("SELECT valid_email,email_opt_out,profile_picture_url,show_tag,preferred_picture_size,show_img,modification_dt FROM user_preference WHERE user_id={0}".format(self.user_id))
            output = self.silo_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.silo_cursor.description]
                self.__make_pretty_and_print__("User Preference", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __get_silo_description__(self):
        with self.Silo_Connect:
            self.silo_cursor.execute("SELECT user_option_value,o.modification_dt FROM user_option o, user_option_reference r WHERE r.user_option_reference_id=o.user_option_reference_id AND user_id={0}".format(self.user_id))
            output = self.silo_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.silo_cursor.description]
                self.__make_pretty_and_print__("Description", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __get_silo_user_activity___(self):
        with self.Silo_Connect:
            self.silo_cursor.execute("SELECT date_type_name, activity_date FROM user_activity_date a, user_date_reference r WHERE a.user_date_reference_id=r.user_date_reference_id AND  user_id={0}".format(self.user_id))
            output = self.silo_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.silo_cursor.description]
                self.__make_pretty_and_print__("User Activity", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __get_silo_new_mobile__(self):
        with self.Silo_Connect:
            self.silo_cursor.execute("SELECT user_id, max(creation_dt) scid_date FROM api_consumer_user WHERE scid in (149831108,149828975,149828744,149828742,149827767,12344321,149827597,149829837,149829932,149830950,149831105) AND user_id={0}".format(self.user_id))
            output = self.silo_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.silo_cursor.description]
                self.__make_pretty_and_print__("New Mobile", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __get_global_user_info__(self):
        with self.User_Master_Connect:
            self.global_cursor.execute("SELECT user_id, username, email, silo_db, subdomain, directory, active, g.creation_dt FROM global_user g, silo_map s WHERE s.silo_id=g.silo and user_id={0}".format(self.user_id))
            output = self.global_cursor.fetchall()
            try:
                field_names = [i[0] for i in self.global_cursor.description]
                self.username = output[0][1]
                self.email= output[0][2]
                self.silodb = output[0][3]
                self.__make_pretty_and_print__("Global User", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise
        
    def __get_delete_info__(self):
        with self.User_Master_Connect:
            self.global_cursor.execute("SELECT user_id, username, email, silo_db, subdomain, directory, active, g.creation_dt FROM delete_global_user g, silo_map s WHERE s.silo_id=g.silo and user_id={0}".format(self.user_id))
            output = self.global_cursor.fetchall()
            if output:
                self.email = output[0][2]
            try:
                field_names = [i[0] for i in self.global_cursor.description]
                self.silodb = output[0][3]
                self.__make_pretty_and_print__("Delete Global User", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __check_for_dup_delete_info_username__(self):
        with self.User_Master_Connect:
            self.global_cursor.execute("SELECT user_id, username, email, silo_db, subdomain, directory, active, g.creation_dt FROM delete_global_user g, silo_map s WHERE s.silo_id=g.silo and username='{0}'".format(self.username))
            output = self.global_cursor.fetchall()
            try:
                field_names = [i[0] for i in self.global_cursor.description]
                self.__make_pretty_and_print__("Delete Global User", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __check_for_dup_delete_info_email__(self):
        with self.User_Master_Connect:
            self.global_cursor.execute("SELECT user_id, username, email, silo_db, subdomain, directory, active, g.creation_dt FROM delete_global_user g, silo_map s WHERE s.silo_id=g.silo and email='{0}'".format(self.email))
            output = self.global_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.global_cursor.description]
                self.__make_pretty_and_print__("Delete Global User", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise


    def __get_global_user_audit__(self):
        with self.User_Master_Connect:
            self.global_cursor.execute("SELECT 'global_user_audit:', action, username, email, modification_dt FROM global_user_audit g WHERE user_id={0};".format(self.user_id))
            output = self.global_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.global_cursor.description]
                self.__make_pretty_and_print__("Global User Audit", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __get_user_delete_queue__(self):
        with self.User_Master_Connect:
            self.global_cursor.execute("SELECT delete_date,actual_delete_datetime,reason FROM user_delete_queue WHERE user_id={0};".format(self.user_id))
            output = self.global_cursor.fetchall()
            try:
                field_names = [i[0] for i in self.global_cursor.description]
                self.__make_pretty_and_print__("User Delete Queue", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __get_payment_info__(self):
        with self.User_Master_Connect:
            self.global_cursor.execute("SELECT payment_id, payment_type, status, payment_date, expiration_date, recurring, amount FROM payment WHERE user_id={0};".format(self.user_id))
            output = self.global_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.global_cursor.description]
                self.__make_pretty_and_print__("Payment", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __get_plus_subscription__(self):
        with self.User_Master_Connect:
            self.global_cursor.execute("SELECT g.plus_subscription_id, user_type_id, payment_type, status, payment_date, expiration_date, recurring, amount FROM plus_subscription g, plus_payment p WHERE g.plus_subscription_id=p.plus_subscription_id AND user_id={0};".format(self.user_id))
            output = self.global_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.global_cursor.description]
                self.__make_pretty_and_print__("Plus Subscription", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __get_delete_payment__(self):
        with self.User_Master_Connect:
            self.global_cursor.execute("SELECT payment_id, payment_type, payment_date, expiration_date FROM delete_payment WHERE user_id={0};".format(self.user_id))
            output = self.global_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.global_cursor.description]
                self.__make_pretty_and_print__("Delete Payment", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __get_comment__(self):
        with self.User_Master_Connect:
            self.global_cursor.execute("SELECT id, user_id from user_comment WHERE user_id={0};".format(self.user_id))
            output = self.global_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.global_cursor.description]
                self.__make_pretty_and_print__("User Comment", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __get_delete_comment__(self):
        with self.User_Master_Connect:
            self.global_cursor.execute("SELECT id, user_id from delete_user_comment WHERE user_id={0};".format(self.user_id))
            output = self.global_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.global_cursor.description]
                self.__make_pretty_and_print__("Delete User Comment", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise

    def __get_global_group__(self):
        with self.User_Master_Connect:
            self.global_cursor.execute("SELECT group_id, group_hash_value, user_id, silo, subdomain, directory, active, g.creation_dt FROM global_group g WHERE user_id={0};".format(self.user_id))
            output = self.global_cursor.fetchall()

            try:
                field_names = [i[0] for i in self.global_cursor.description]
                self.__make_pretty_and_print__("Global Group", field_names, output)
            except IndexError, i:
                i = sys.exc_info()[0]
            except:
                print "Error: ", sys.exc_info[0]
                raise


if options.user_id:
    user_id = options.user_id
    report = user_report(user_id, False)
    if options.cmod == True:
        report.get_cmod_report()
    elif options.all == True:
        report.get_full_report()    
    else:
        report.get_report()

if options.username:
    field = "username"
    user_id = get_user_id(field, options.username)
    report = user_report(user_id, True)
    if options.cmod == True:
        report.get_cmod_report()
    elif options.all == True:
        report.get_full_report()    
    else:
        report.get_report()

if options.email:
    field = "email"
    user_id = get_user_id(field, options.email)
    report = user_report(user_id, True)
    if options.cmod == True:
        report.get_cmod_report()
    elif options.all == True:
        report.get_full_report()    
    else:
        report.get_report()


