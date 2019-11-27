# coding=utf-8
from parser import *
from dbmanager import *
from database import *
from settings import *
from execute_create import *
from database import *
from os import *
from os.path import *
import pickle

def load_dbmanager():
    dbmanager = Dbmanager()
    dbmanager_path = os.path.join(os.getcwd(), DB_PATH)
    dbs = [dir for dir in listdir(dbmanager_path) if isdir(join(dbmanager_path, dir))]
    for db in dbs:
        database = Database(db)
        dbmanager.dbs.append(database)
        current_db_path = os.path.join(dbmanager_path, db)
        #print(current_db_path)
        relations = [relation for relation in listdir(current_db_path) if isfile(join(current_db_path, relation))]
        #print(relations)
        for relation in relations:
            current_relation_path = os.path.join(current_db_path, relation)
            with open(current_relation_path, 'rb') as input:
                relation = pickle.load(input)
                database.relations.append(relation)
    return dbmanager

def save_dbmanager(dbmanager):
    dbmanager_path = os.path.join(os.getcwd(), DB_PATH)
    for db in dbmanager.dbs:
        db_path = os.path.join(dbmanager_path, str(db.name))
        for relation in db.relations:
            relation_path = os.path.join(db_path, str(relation.name))
            with open(relation_path, 'wb') as output:
                pickle.dump(relation, output, pickle.HIGHEST_PROTOCOL)

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

main()