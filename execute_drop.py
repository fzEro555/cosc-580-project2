# coding=utf-8
from dbmanager import *
from table import *
import re
from settings import *
from os import *

def drop_table(tokens, dbmanager):
    if dbmanager.current_db == None:
        print("Error: No database selected")
    table_name = tokens[2]
    current_db = dbmanager.get_current_db()
    if not current_db.relation_exists(table_name):
        print("Error: Relation not exists")
    else:
        root_directory = os.path.join(os.getcwd(), DB_PATH)
        current_db_directory = os.path.join(root_directory, str(current_db.name))
        for relation in current_db.relations:
            if relation.name == table_name.upper():
                current_db.relations.remove(relation)
                table_file = os.path.join(current_db_directory, str(table_name.upper()))
                if os.path.isfile(table_file):
                    os.remove(table_file)
    return dbmanager

def drop_index(tokens, dbmanager):
    if dbmanager.current_db == None:
        print("Error: No database selected")
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
                    else:
                        print("Error: Index not exists")
        else:
            print("Error: Relation not exists")
    else:
        print("Error: Syntax error")
    return dbmanager