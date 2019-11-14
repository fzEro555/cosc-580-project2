# coding=utf-8
from database import *
from dbmanager import *

class Parser:
    def __init__(self):
        self.database = None

    def parse_sql(self,sql,dbmanager):
        i = 0
        evaluate_flag = False
        sql = sql.replace(';', '')
        sql_tokens = sql.split("")
        sql_tokens[:] = [token.lower() for token in sql_tokens]
        first_token = sql_tokens[i]

        if first_token == "create":
            # create a new database
            if sql_tokens[i+1] == "database":
                new_database = Database(sql_tokens[i+2])
                #self.database = Database(sql_tokens[i+2])
                if not dbmanager.db_exists(new_database):
                    dbmanager.add_db(new_database)
                else:
                    print("Error: Database %s already exists" %sql_tokens[i+2])
            # create a new table in an existing database
            elif sql_tokens[i+1] == "table":
                self.parse_create_table(i, sql_tokens)
                if evaluate_flag:
                    pass
            # create an index
            elif sql_tokens[i+1] == "index":
                self.parse_create_index(i, sql_tokens)
                if evaluate_flag:
                    pass
            else:
                print("Error: Syntax error")

        elif first_token == "drop":
            # drop a table
            if sql_tokens[i+1] == "table":
                self.parse_drop_table(i, sql_tokens)
                if evaluate_flag:
                    pass
            # drop an index
            elif sql_tokens[i+1] == "index":
                self.parse_drop_index(i, sql_tokens)
                if evaluate_flag:
                    pass
            else:
                print("Error: Syntax error")

        elif first_token == "insert":
            if sql_tokens[i+1] == "into":
                self.parse_insert(i, sql_tokens)
                if evaluate_flag:
                    pass
            else:
                print("Error: Syntax error")

        elif first_token == "delete":
            if sql_tokens[i+1] == "from":
                self.parse_delete(i, sql_tokens)
                if evaluate_flag:
                    pass
            else:
                print("Error: Syntax error")

        elif first_token == "update":
            self.parse_update(i, sql_tokens)
            if evaluate_flag:
                pass

        elif first_token == "select":
            self.parse_select()
            if evaluate_flag:
                pass

        # use a database
        elif first_token == "use":
            if sql_tokens[i+1] == "database":
                if dbmanager.db_exists(sql_tokens[i+2]):
                    self.database = dbmanager.get_db(sql_tokens[i+2])
                else:
                    print("Error: Database %s not exists" %sql_tokens[i+2])
            else:
                print("Error: Syntax error")

    def parse_create_table(self, i, tokens):
        pass

    def parse_create_index(self, i, tokens):
        pass

    def parse_drop_table(self, i, tokens):
        pass

    def parse_drop_index(self, i, tokens):
        pass

    def parse_insert(self, i, tokens):
        pass

    def parse_delete(self, i, tokens):
        pass

    def parse_update(self, i, tokens):
        pass

    def parse_select(self, i, tokens):
        pass

