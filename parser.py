# coding=utf-8
from database import *
from dbmanager import *
from handler import *
def parse_sql(sql,dbmanager):
    i = 0
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
    first_token = sql_tokens[i]

    if first_token == "create":
        # create a new database
        if sql_tokens[i+1] == "database":
            new_dbname = sql_tokens[i+2]
            if not dbmanager.db_exists(new_dbname):
                new_database = Database(new_dbname)
                directory = os.path.join('./Databases', new_dbname)
                os.path.mkdir(directory)
                dbmanager.add_db(new_database)
            else:
                print("Error: Database %s already exists" %sql_tokens[i+2])
        # create a new table in an existing database
        elif sql_tokens[i+1] == "table":
            create_table(i, sql_tokens, dbmanager)
            if evaluate_flag:
                pass
        # create an index
        elif sql_tokens[i+1] == "index":
            parse_create_index(i, sql_tokens)
            if evaluate_flag:
                pass
        else:
            print("Error: Syntax error")

    elif first_token == "drop":
        # drop a table
        if sql_tokens[i+1] == "table":
            parse_drop_table(i, sql_tokens)
            if evaluate_flag:
                pass
        # drop an index
        elif sql_tokens[i+1] == "index":
            parse_drop_index(i, sql_tokens)
            if evaluate_flag:
                pass
        else:
            print("Error: Syntax error")

    elif first_token == "insert":
        if sql_tokens[i+1] == "into":
            parse_insert(i, sql_tokens)
            if evaluate_flag:
                pass
        else:
            print("Error: Syntax error")

    elif first_token == "delete":
        if sql_tokens[i+1] == "from":
            parse_delete(i, sql_tokens)
            if evaluate_flag:
                pass
        else:
            print("Error: Syntax error")

    elif first_token == "update":
        parse_update(i, sql_tokens)
        if evaluate_flag:
            pass

    elif first_token == "select":
        parse_select()
        if evaluate_flag:
            pass

    # use a database
    elif first_token == "use":
        if sql_tokens[i+1] == "database":
            if dbmanager.db_exists(sql_tokens[i+2]):
                dbmanager.set_current_db(dbmanager.get_db(sql_tokens[i+2]))
            else:
                print("Error: Database %s not exists" %sql_tokens[i+2])
        else:
            print("Error: Syntax error")

    elif first_token == "exit":
        print("See you~")

    return dbmanager

def parse_create_index(i, tokens):
    pass

def parse_drop_table(i, tokens):
    pass

def parse_drop_index(i, tokens):
    pass

def parse_insert(i, tokens):
    pass

def parse_delete(i, tokens):
    pass

def parse_update(i, tokens):
    pass

def parse_select(i, tokens):
    pass

