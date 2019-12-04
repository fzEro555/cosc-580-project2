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
            with open(relation_path, 'wb') as output:
                pickle.dump(relation, output, pickle.HIGHEST_PROTOCOL)


def load_relation():
    dbmanager = Dbmanager()
    new_database = Database("demo")
    root_directory = os.path.join(os.getcwd(), DB_PATH)
    new_dbdir = os.path.join(root_directory, new_database.name)
    # make a new db directory
    os.mkdir(new_dbdir)

    r1 = Table(1001, 2)
    r1.set_name("relation1")
    r1.set_attributes(["col1","col2"])
    np.put(r1.storage, 0, "col1")
    np.put(r1.storage, 1, "col2")

    for row in range(1,r1.row_number):
        for col in range(0,r1.col_number):
            np.put(r1.storage,row * r1.col_number + col, row)
    new_database.add_relation(r1)

    r2 = Table(1001, 2)
    r2.set_name("relation2")
    r2.set_attributes(["col1", "col3"])
    np.put(r2.storage, 0, "col1")
    np.put(r2.storage, 1, "col3")
    for row in range(1,r2.row_number):
        np.put(r2.storage, row*r2.col_number, row)
        np.put(r2.storage, row * r2.col_number + 1, 1)
    new_database.add_relation(r2)

    r3 = Table(10001, 2)
    r3.set_name("relation3")
    r3.set_attributes(["col1", "col4"])
    np.put(r3.storage, 0, "col1")
    np.put(r3.storage, 1, "col4")

    for row in range(1, r3.row_number):
        for col in range(0, r3.col_number):
            np.put(r3.storage, row * r3.col_number + col, row)
    new_database.add_relation(r3)

    r4 = Table(10001, 2)
    r4.set_name("relation4")
    r4.set_attributes(["col1", "col5"])
    np.put(r4.storage, 0, "col1")
    np.put(r4.storage, 1, "col5")
    for row in range(1, r4.row_number):
        np.put(r4.storage, row * r4.col_number, row)
        np.put(r4.storage, row * r4.col_number + 1, 1)
    new_database.add_relation(r4)

    dbmanager.add_db(new_database)
    return dbmanager

def main():
    #dbmanager = load_relation()
    dbmanager = load_dbmanager()
    cmd = ""

    while cmd != "exit":
        cmd = input()
        dbmanager = parse_sql(cmd, dbmanager)
        save_dbmanager(dbmanager)

main()