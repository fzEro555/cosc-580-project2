# coding=utf-8
from dbmanager import *
from table import *
import re
from settings import *
from os import *

def drop_table(tokens, dbmanager):
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
                if (os.path.isfile(table_file)):
                    os.remove(table_file)

def drop_index(tokens, dbmanager):
    pass