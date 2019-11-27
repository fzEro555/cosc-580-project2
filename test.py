# coding=utf-8
import re
from table import *
from dbmanager import *
from execute_create import *
def getAttrCons(attrsCons):
    attrs = attrsCons.split(",")
    i=0
    length = len(attrs)
    while i < length-1:
        if "(" in attrs[i] and ")" not in attrs[i] and ")" in attrs[i+1] and "(" not in attrs[i+1]:
            attrs[i] +=","+ attrs[i+1]
            attrs.remove(attrs[i+1])
        i += 1
        length = len(attrs)
    return attrs

keywords = ["table","char","int","varchar","decimal","date"]

def create_index(tokens, i):
    parseError = False
    if (tokens[i] == "if" and tokens[i+1] == "not"):
        ifNotExistsCheck = True
        i+=3
    index_name = tokens[i]
    i+=1
    # Sanity check
    if tokens[i] == "on":
        table_name = tokens[i+1] # ASSUMPTION: parens are not own tokens
        column_list = []
        temp_vals = tokens[i+2:]
        for tok in temp_vals:
            new_tok = tok
            if '(' in tok:
                new_tok = new_tok.replace('(', '')
            if ')' in tok:
                new_tok = new_tok.replace(')', '')
            if ',' in tok:
                new_tok = new_tok.replace(',', '')
            column_list.append(new_tok)
        #print(column_list)
        indices = []


        indices.append((index_name, column_list))
        print(indices)
        for index in indices:
            print("in" == index[0])
            print(index[1])
    else:
        return index_name,"", [], i, True

if __name__ == "__main__":
    index = 0
    attrnames = []
    primary = []
    sql = "CREATE INDEX IN ON STUDENT (STU_NUM, stu_name);"
    dbmanager = Dbmanager()
    sql = sql.replace(';', '')
    while sql.find("'") != -1:
        sql = sql.replace("'", "")
    while sql.find('\t') != -1:
        sql = sql.replace("\t", " ")
    while sql.find('\n') != -1:
        sql = sql.replace("\n", " ")
    sql_tokens = sql.split(" ")
    sql_tokens[:] = [token.lower() for token in sql_tokens]
    #create_database(0, sql_tokens, dbmanager)
    reg = "\((.*)\)"
    sql = ' '.join(sql_tokens)
    attributes_string = re.compile(reg).findall(sql)
    #print(attributes_string[0])
    attributes_string = attributes_string[0]
    attributes_string = attributes_string.split(", ")
    #print(attributes_string)
    create_index(sql_tokens, 2)
