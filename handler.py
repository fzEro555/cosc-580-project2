# coding=utf-8
from dbmanager import *
def create_table(i, tokens, dbmanager):
    table_name = tokens[i + 2]
    if dbmanager.get_current_db().relation_exists(table_name):
        print("Error: Relation already exists")
    else:
        reg = "\((.*)\)"
        sql = ' '.join(tokens)

def restore_dbmanager():
    dbmanager = Dbmanager()
    return dbmanager