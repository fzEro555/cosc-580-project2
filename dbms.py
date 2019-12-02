# coding=utf-8
from parser import *
from dbmanager import *
from database import *
from settings import *
from execute_create import *
from database import *
import os
import io
from os import listdir
from os.path import isfile, join, isdir
import numpy as np
import pickle
import jsonpickle
import jsonpickle.ext.numpy as jsonpickle_numpy
jsonpickle_numpy.register_handlers()
jsonpickle.handlers.registry.register(np.chararray, base=True)

# load the existence dbmanager
def load_dbmanager():
    dbmanager = Dbmanager()
    dbmanager_path = os.path.join(os.getcwd(), DB_PATH)
    # get the directory of each database
    dbs = [dir for dir in listdir(dbmanager_path) if isdir(join(dbmanager_path, dir))]
    for db in dbs:
        database = Database(db)
        dbmanager.dbs.append(database)
        current_db_path = os.path.join(dbmanager_path, db)
        #print(current_db_path)
        # get the relations in each database
        relations = [relation for relation in listdir(current_db_path) if isfile(join(current_db_path, relation))]
        #print(relations)
        for relation in relations:
            current_relation_path = os.path.join(current_db_path, relation)
            '''
            with io.open(current_relation_path) as input:
                relation = jsonpickle.decode(input.read())
                database.relations.append(relation)
            '''
            with io.open(current_relation_path, 'rb') as input:
                relation = pickle.load(input)
                database.relations.append(relation)
    return dbmanager


# change the state of dbmanager and save it
def save_dbmanager(dbmanager):
    dbmanager_path = os.path.join(os.getcwd(), DB_PATH)
    for db in dbmanager.dbs:
        db_path = os.path.join(dbmanager_path, str(db.name))
        for relation in db.relations:
            relation_path = os.path.join(db_path, str(relation.name))
            '''
            with io.open(relation_path, 'w') as output:
                output.write(jsonpickle.encode(relation))
            '''
            with open(relation_path, 'wb') as output:
                pickle.dump(relation, output, pickle.HIGHEST_PROTOCOL)

def main():
    dbmanager = load_dbmanager()

    cmd = ""

    while cmd != "exit":
        cmd = input()
        dbmanager = parse_sql(cmd, dbmanager)
        save_dbmanager(dbmanager)

main()