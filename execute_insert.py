# coding=utf-8
from dbmanager import *
from table import *
from database import *
import re
import copy

def insert_into(tokens, dbmanager):
    if dbmanager.current_db == None:
        print("Error: No database selected")
        return dbmanager
    else:
        # get the table name
        table_name = tokens[2]
        current_db = dbmanager.get_current_db()
        if not current_db.relation_exists(table_name):
            print("Error: Relation not exists")
            return dbmanager
        else:
            reg = "\((.*)\)"
            sql = ' '.join(tokens)
            values_string = re.compile(reg).findall(sql)
            values_string = values_string[0]
            values = values_string.split(", ")
            table = current_db.get_relation(table_name)
            attributes = table.attributes
            primary_key = table.primary_key
            row_number = table.row_number
            col_number = table.col_number
            # get the indexes of columns of primary keys
            i = 0
            primary_key_indexes = []
            while i < len(attributes):
                for key in primary_key:
                    if attributes[i] == key:
                        primary_key_indexes.append(i)
                i += 1
            temp_data = table.storage.tolist()
            values_primary_key = []
            for index in primary_key_indexes:
                values_primary_key.append(values[index])
            # compare every row's primary key value with the insert row's primary key
            for row in temp_data:
                row_primary_key = []
                for index in primary_key_indexes:
                    row_primary_key.append(row[index].decode('utf-8'))
                if values_primary_key == row_primary_key:
                    print("Error: insert a duplicate tuple is not allowed")
                    return dbmanager
            # maybe don't need to copy. not sure right now
            new_table = copy.deepcopy(table)
            new_table.storage.resize((row_number+1, col_number))
            table.set_row_number(row_number+1)
            col_indexes = []
            j = 0
            while j < col_number:
                col_indexes.append(j)
                j += 1
            for x in range(0, len(values)):
                np.put(new_table.storage, row_number * col_number + col_indexes[x], values[x])
            table.storage = new_table.storage
            print("Insert data into table %s successfully" %table_name.upper())
            return dbmanager