import sqlite3
import sys, os, time, atexit
import threading
import time
import logging
import socket
from collections import *
import queue as queue
from concurrent.futures import *
import time
from datetime import datetime
from toolz import partition_all



class python_sqlite:

    def __init__(self):
        self.bulk_insert_entries = 100000
        self.blacklist_urls = []
        self.db = sqlite3.connect('blacklist.sqlite')
        # self.db = sqlite3.connect(":memory:")
        self.cursor = self.db.cursor()
        self.db.execute('pragma journal_mode=DELETE')
        # self.db.execute('pragma journal_mode=memory')
        # self.db.execute('pragma journal_mode=wal')
        self.db.execute('pragma SYNCHRONOUS=1')
        self.db.execute('pragma PAGE_SIZE=4096')
        self.db.execute('pragma cache_size = 8192')
        self.db.execute('DROP table if exists blacklist')
        self.db.execute('CREATE TABLE blacklist (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, url TEXT NOT NULL UNIQUE)')
        # sqlite> PRAGMA auto_vacuum = None(0), FULL (1), INCREMENTAL(2)
        self.db.execute('pragma auto_vacuum=1')


    def __del__(self):
        self.db.close()


    def read_file(self, filepath):
        urls = []
        file = open(filepath, "r", encoding="utf8", errors='ignore')
        for x in file:
            y = x.lstrip(".-")
            y = y.rstrip(".-\n")
            # tup = ('url':'y')
            urls.append({'url':y})
            # print (y)
        file.close()
        return urls


    def read_blacklist_files(self):
        start_time = datetime.now()

        for x in range(1, 8):
            file_name = "blacklist" + str(x) + ".db"
            # print(file_name)
            urls_list = self.read_file(file_name)
            # extend appens list to another
            self.blacklist_urls.extend(urls_list)

        time_elapsed = datetime.now() - start_time
        print('read_blacklist_files:: total time for {} entries = {}'.format(len(self.blacklist_urls), time_elapsed))


    def add_blacklist_url(self, urls):
        # print('add_blacklist_url:: entries = {}'.format(len(urls)))
        start_time = datetime.now()
        records = len(urls)
        while True:
            if not urls:
                break

            templist = urls[-self.bulk_insert_entries:]
            del urls[-self.bulk_insert_entries:]
            try:
                start_commit = datetime.now()
                # self.cursor.execute('begin')
                self.cursor.executemany('''INSERT OR IGNORE INTO blacklist(url) VALUES(:url)''', (templist))
                # self.cursor.execute('commit')
                end_commit = datetime.now() - start_commit
                print('add_blacklist_url:: total time for INSERT OR IGNORE INTO blacklist {} entries = {}'.format(len(templist), end_commit))
            except sqlite3.Error as e:
                # self.cursor('rollback')
                print("add_blacklist_url:: Database error: %s" % e)
            except Exception as e:
                # self.cursor('rollback')
                print("add_blacklist_url:: Exception in _query: %s" % e)
        self.db.commit()
        time_elapsed = datetime.now() - start_time
        print('add_blacklist_url:: total time for {} entries = {}'.format(records, time_elapsed))
        # self.db.execute('pragma auto_vacuum=1')



    def add_blacklist_url_DanD(self, urls):
        # print('add_blacklist_url:: entries = {}'.format(len(urls)))
        start_time = datetime.now()
        records = len(urls)
        self.cursor.execute('begin')
        while True:
            if not urls:
                break

            templist = urls[-self.bulk_insert_entries:]
            del urls[-self.bulk_insert_entries:]
            try:
                start_commit = datetime.now()
                self.cursor.executemany('''INSERT OR IGNORE INTO blacklist(url) VALUES(:url)''', (templist))
                end_commit = datetime.now() - start_commit
                print('add_blacklist_url:: total time for INSERT OR IGNORE INTO blacklist {} entries = {}'.format(len(templist), end_commit))
            except sqlite3.Error as e:
                print("add_blacklist_url:: Database error: %s" % e)
            except Exception as e:
                print("add_blacklist_url:: Exception in _query: %s" % e)
        self.cursor.execute('commit')
        self.db.commit()
        time_elapsed = datetime.now() - start_time
        print('add_blacklist_url:: total time for {} entries = {}'.format(records, time_elapsed))



    def add_blacklist_url_Javier(self, urls):
        records = len(urls)
        # print('add_blacklist_url:: entries = {}'.format(len(urls)))
        start_time = datetime.now()
        for batch in partition_all(self.bulk_insert_entries, urls):
            try:
                start_commit = datetime.now()
                self.cursor.executemany('''INSERT OR IGNORE INTO blacklist(url) VALUES(:url)''', batch)
                end_commit = datetime.now() - start_commit
                print('add_blacklist_url:: total time for INSERT OR IGNORE INTO blacklist {} entries = {}'.format(
                    len(batch), end_commit))
            except sqlite3.Error as e:
                print("add_blacklist_url:: Database error: %s" % e)
            except Exception as e:
                print("add_blacklist_url:: Exception in _query: %s" % e)
            self.db.commit()
        time_elapsed = datetime.now() - start_time
        print('add_blacklist_url:: total time for {} entries = {}'.format(records, time_elapsed))



pysql = python_sqlite()
pysql.read_blacklist_files()
pysql.add_blacklist_url(pysql.blacklist_urls)
