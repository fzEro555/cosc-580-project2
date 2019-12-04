# coding=utf-8
from database import *
from dbmanager import *
from execute_drop import *
from execute_create import *
from execute_insert import *
from execute_delete import *
from execute_update import *
from execute_select import *
from utils import *

def parse_sql(sql,dbmanager):
    evaluate_flag = False
    sql = sql.replace(';', '')
    while sql.find("'") != -1:
        sql = sql.replace("'", "")
    while sql.find('\t') != -1:
        sql = sql.replace("\t", " ")
    while sql.find('\n') != -1:
        sql = sql.replace("\n", " ")
    sql_tokens = sql.split(" ")
    sql_tokens[:] = [token.lower() for token in sql_tokens]
    first_token = sql_tokens[0]

    if first_token == "create":
        # create a new database
        if sql_tokens[1] == "database":
            dbmanager = create_database(sql_tokens, dbmanager)
        # create a new table in an existing database
        elif sql_tokens[1] == "table":
            dbmanager = create_table(sql_tokens, dbmanager)
        # create an index
        elif sql_tokens[1] == "index":
            dbmanager = create_index(sql_tokens, dbmanager)
        else:
            print("Error: Syntax error")

    elif first_token == "drop":
        # drop a database
        if sql_tokens[1] == "database":
            dbmanager = drop_database(sql_tokens, dbmanager)
        # drop a table
        elif sql_tokens[1] == "table":
            dbmanager = drop_table(sql_tokens, dbmanager)
        # drop an index
        elif sql_tokens[1] == "index":
            dbmanager = drop_index(sql_tokens, dbmanager)
        else:
            print("Error: Syntax error")

    elif first_token == "insert":
        if sql_tokens[1] == "into":
            dbmanager = insert_into(sql_tokens, dbmanager)
        else:
            print("Error: Syntax error")

    elif first_token == "delete":
        if sql_tokens[1] == "from":
            dbmanager = delete_from(sql_tokens, dbmanager)
        else:
            print("Error: Syntax error")

    elif first_token == "update":
        dbmanager = update(sql_tokens, dbmanager)

    elif first_token == "select":
        sql_temp = ' '.join(sql_tokens)
        attributes = parse_select(sql_temp)
        tables = parse_from(sql_temp)
        conditions = parse_where(sql_temp)
        select_from(attributes, tables, conditions, dbmanager)

    elif first_token == "show":
        # show table
        if sql_tokens[1] == "table":
            if dbmanager.current_db == None:
                print("Error: No database selected")
            else:
                table_name = sql_tokens[2]
                table = dbmanager.get_current_db().get_relation(table_name)
                if table:
                    print(table.name)
                    print(table.storage)
                else:
                    print("Error: Table %s not exists" &table_name.upper())
        # show database
        elif sql_tokens[1] == "databases":
            for db in dbmanager.dbs:
                print(db.name)
        # show database
        elif sql_tokens[1] == "database":
            db = dbmanager.get_db(sql_tokens[2])
            for table in db.relations:
                print(table.name)
        else:
            print("Error: Syntax error")

    # use a database
    elif first_token == "use":
        if sql_tokens[1] == "database":
            if dbmanager.db_exists(sql_tokens[2]):
                dbmanager.set_current_db(dbmanager.get_db(sql_tokens[2]))
                print("The current database is %s" %(sql_tokens[2]).upper())
            else:
                print("Error: Database %s not exists" %(sql_tokens[2]).upper())
        else:
            print("Error: Syntax error")

    elif first_token == "exit":
        print("See you~")

    return dbmanager