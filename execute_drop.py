# coding=utf-8
from dbmanager import *
from table import *
from settings import *
import os
import shutil

def drop_database(tokens, dbmanager):
    # get the db name to drop
    drop_dbname = tokens[2]
    if dbmanager.db_exists(drop_dbname):
        drop_database = dbmanager.get_db(drop_dbname)
        root_directory = os.path.join(os.getcwd(), DB_PATH)
        drop_dbdir = os.path.join(root_directory, drop_database.name)
        # remove the db directory
        shutil.rmtree(drop_dbdir)
        dbmanager.rm_db(drop_database)
        print("Database %s is dropped successfully" % drop_dbname.upper())
        return dbmanager
    else:
        print("Error: Database %s not exists" % drop_dbname.upper())
        return dbmanager


def drop_table(tokens, dbmanager):
    if dbmanager.current_db == None:
        print("Error: No database selected")
        return dbmanager
    table_name = tokens[2]
    current_db = dbmanager.get_current_db()
    if not current_db.relation_exists(table_name):
        print("Error: Relation not exists")
        return dbmanager
    else:
        root_directory = os.path.join(os.getcwd(), DB_PATH)
        current_db_directory = os.path.join(root_directory, str(current_db.name))
        for relation in current_db.relations:
            if relation.name == table_name.upper():
                current_db.relations.remove(relation)
                table_file = os.path.join(current_db_directory, str(table_name.upper()))
                if os.path.isfile(table_file):
                    os.remove(table_file)
                    print("Table %s dropped successfully" %table_name.upper())
        return dbmanager

def drop_index(tokens, dbmanager):
    if dbmanager.current_db == None:
        print("Error: No database selected")
        return dbmanager
    else:
        index_name = tokens[2]
        current_db = dbmanager.get_current_db()
        if tokens[3] == "on":
            table_name = tokens[4]
            if current_db.relation_exists(table_name):
                for relation in current_db.relations:
                    if relation.name == table_name.upper():
                        if relation.index_exists(index_name):
                            for tuple in relation.indexes:
                                if tuple[0] == index_name:
                                    relation.indexes.remove(tuple)
                                    print("Index %s dropped successfully" %index_name)
                                    return dbmanager
                        else:
                            print("Error: Index not exists")
                            return dbmanager
            else:
                print("Error: Relation not exists")
                return dbmanager
        else:
            print("Error: Syntax error")
            return dbmanager