# coding=utf-8
from parser import *
from dbmanager import *
from database import *
from execute_create import *

def load_dbmanager():
    dbmanager = Dbmanager()
    return dbmanager
def save_dbmanager(dbmanager):
    pass

def main():
    #database = load_relations()
    #database = restore_state()

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
         save_dbmanager() # save state even if error occurs
         print("An unknown error occurred.")




main()