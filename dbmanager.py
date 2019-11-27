# coding=utf-8
from database import *
import os

class Dbmanager:
    def __init__(self):
        self.dbs = []
        self.db_num = 0
        self.current_db = None

    def set_current_db(self,db):
        self.current_db = db

    def get_current_db(self):
        return self.current_db

    def add_db(self, db):
        self.dbs.append(db)
        self.db_num += 1

    def get_db(self, db_name):
        for db in self.dbs:
            if db.name == db_name.upper():
                return db

    def db_exists(self, db_name):
        for db in self.dbs:
            if db.name == db_name.upper():
                return True
        return False

    def rm_db(self, db):
        if self.db_exists(db.name):
            self.dbs.remove(db)
            self.db_num -= 1