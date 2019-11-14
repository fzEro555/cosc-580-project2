# coding=utf-8
from parser import *
from dbmanager import *
from database import *
from evaluator import *

def main():
    #database = load_relations()
    #database = restore_state()

    dbmanager = restore_dbmanager()
    cmd = ""
    prompt = "> "
    cmd_list = []
    parser = Parser()
    try:
        while cmd != "quit":
            cmd = input(prompt)
            cmd_list.append(cmd)
            database,tokens = parser.parse_sql(cmd, dbmanager)
            save_state(database)
    except:
         save_state(database) # save state even if error occurs
         print("An unknown error occurred.")




main()