# coding=utf-8
from parser import *
from dbmanager import *
from database import *
from settings import *
from execute_create import *
from os import *
from os.path import *

def load_dbmanager():
    dbmanager = Dbmanager()
    dbmanager_path = os.path.join(os.getcwd(), DB_PATH)
    dbs = [dir for dir in listdir(dbmanager_path) if isfile(join(dbmanager, dir))]
    #return dbmanager

def save_dbmanager(dbmanager):
    pass

def main():
    dbmanager = load_dbmanager()
    cmd = ""
    prompt = "> "
    cmd_list = []
    try:
        while cmd != "exit":
            cmd = input(prompt)
            cmd_list.append(cmd)
            dbmanager = parse_sql(cmd, dbmanager)
            save_dbmanager()
    except:
         # save state even if error occurs
         save_dbmanager()
         print("An unknown error occurred.")

load_dbmanager()