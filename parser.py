# coding=utf-8
from database import *
from dbmanager import *
from execute_create import *
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
        # drop a table
        if sql_tokens[1] == "table":
            parse_drop_table(sql_tokens)
            if evaluate_flag:
                pass
        # drop an index
        elif sql_tokens[1] == "index":
            parse_drop_index(sql_tokens)
            if evaluate_flag:
                pass
        else:
            print("Error: Syntax error")

    elif first_token == "insert":
        if sql_tokens[1] == "into":
            parse_insert(sql_tokens)
            if evaluate_flag:
                pass
        else:
            print("Error: Syntax error")

    elif first_token == "delete":
        if sql_tokens[1] == "from":
            parse_delete(sql_tokens)
            if evaluate_flag:
                pass
        else:
            print("Error: Syntax error")

    elif first_token == "update":
        parse_update(sql_tokens)
        if evaluate_flag:
            pass

    elif first_token == "select":
        parse_select()
        if evaluate_flag:
            pass

    # use a database
    elif first_token == "use":
        if sql_tokens[1] == "database":
            if dbmanager.db_exists(sql_tokens[2]):
                dbmanager.set_current_db(dbmanager.get_db(sql_tokens[2]))
            else:
                print("Error: Database %s not exists" %sql_tokens[2])
        else:
            print("Error: Syntax error")

    elif first_token == "exit":
        print("See you~")

    return dbmanager



def parse_drop_table(tokens):
    pass

def parse_drop_index(tokens):
    pass

def parse_insert(tokens):
    pass

def parse_delete(tokens):
    pass

def parse_update(tokens):
    pass

def parse_select(tokens):
    pass

