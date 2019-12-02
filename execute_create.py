# coding=utf-8
from dbmanager import *
from database import *
from table import *
import re
from settings import *
import os

def get_attribute_constrain(attrsCons):
    attributes = attrsCons.split(",")
    i=0
    length = len(attributes)
    while i < length-1:
        if "(" in attributes[i] and ")" not in attributes[i] and ")" in attributes[i+1] and "(" not in attributes[i+1]:
            attributes[i] += "," + attributes[i+1]
            attributes.remove(attributes[i+1])
        i += 1
        length = len(attributes)
    return attributes

def create_database(tokens, dbmanager):
    # get the db name
    new_dbname = tokens[2]
    if not dbmanager.db_exists(new_dbname):
        new_database = Database(new_dbname)
        root_directory = os.path.join(os.getcwd(), DB_PATH)
        new_dbdir = os.path.join(root_directory, new_database.name)
        # make a new db directory
        os.mkdir(new_dbdir)
        dbmanager.add_db(new_database)
        print("Database %s is created successfully" % new_dbname)
        return dbmanager
    else:
        print("Error: Database %s already exists" % new_dbname)
        return dbmanager

def create_table(tokens, dbmanager):
    if dbmanager.current_db == None:
        print("Error: No database selected")
        return dbmanager
    else:
        current_db= dbmanager.get_current_db()
        # get the table name
        table_name = tokens[2]
        if current_db.relation_exists(table_name):
            print("Error: Relation already exists")
            return dbmanager
        else:
            primary_key = []
            attribute_names = []
            reg = "\((.*)\)"
            sql = ' '.join(tokens)
            attributes_string = re.compile(reg).findall(sql)
            attributes = get_attribute_constrain(attributes_string[0])
            for attribute in attributes:
                attribute = attribute.lstrip()
                # get the primary key
                reg = "primary\s*key.*\((.*)\)+"
                primary = re.compile(reg).findall(attribute)
                if len(primary) > 0:
                    primary = primary[0]
                    primary_key = primary.split(', ')

                # get attributes
                else:
                    attribute = attribute.split(' ')
                    attribute_names.append(attribute[0])

            # create a table, the first row contains attribute names
            row_number = 1
            col_number = len(attribute_names)
            table = Table(row_number, col_number)
            table.set_name(table_name)
            table.set_primary_key(primary_key)
            table.set_attributes(attribute_names)
            table.storage.fill(0)
            for x in range(0, table.col_number):
                np.put(table.storage, x, attribute_names[x])
            # add the table into the current database
            current_db.add_relation(table)
            print("Table %s is created successfully" %table.name)
            return dbmanager

def create_index(tokens, dbmanager):
    if dbmanager.current_db == None:
        print("Error: No database selected")
        return dbmanager
    else:
        index_name = tokens[2]
        if tokens[3] == "on":
            table_name = tokens[4]
            reg = "\((.*)\)"
            sql = ' '.join(tokens)
            attributes_string = re.compile(reg).findall(sql)
            attributes = attributes_string[0]
            attributes = attributes.split(", ")
            if dbmanager.current_db.relation_exists(table_name):
                table = dbmanager.current_db.get_relation(table_name)
                if not table.index_exists(index_name):
                    table.add_index(index_name, attributes)
                    print("Index %s is created successfully" %index_name)
                    return dbmanager
                else:
                    print("Error: Index already exists")
                    return dbmanager
            else:
                print("Error: Relation not exists")
                return dbmanager
        else:
            print("Error: Syntax error")
            return dbmanager