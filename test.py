# coding=utf-8
import re
from table import *
from dbmanager import *
from execute_create import *
import os
import io
from table import *
import jsonpickle
from collections import OrderedDict
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
statement_tag = ["select", "from", "where", "group by", "order by"]


def rmListSpace(list):
    while "" in list:
        list.remove("")
    return list


def next_tag(sql,current_tag):
    index = sql.find(current_tag, 0)
    for tag in statement_tag:
        if sql.find(tag, index+len(current_tag)) != -1:
            return tag
    return ""


def rm_str_space(string):
    list = string.split(" ")
    list = rmListSpace(list)
    result = ''
    for word in list:
        result += word+" "
    return result[0:-1]

def parseWhere(sql):
    conditions = []
    if "where " not in sql:
        return None
    next_keyword = next_tag(sql, "where")
    reg = "where (.+)"+next_keyword
    where = re.compile(reg).findall(sql)[0]
    where_statement = where.split(" and ")
    for and_statement in where_statement:
        and_statement = and_statement.split(" or ")
        for or_statement in and_statement:
            if "=" in or_statement:
                compare_keyword = "="
            elif "<" in or_statement:
                compare_keyword = "<"
            elif ">" in or_statement:
                compare_keyword = ">"
            elif "<=" in or_statement:
                compare_keyword = "<="
            elif ">=" in or_statement:
                compare_keyword = ">="
            reg = "^(.+)\s*"+compare_keyword
            attr = re.compile(reg).findall(or_statement)[0]
            attr = rm_str_space(attr)
            reg = compare_keyword+"\s*(.+)$"
            value = re.compile(reg).findall(or_statement)[0]
            value = rm_str_space(value)
            conditions.append(tuple([attr, compare_keyword, value]))
            conditions.append('or')
        conditions.pop()
        conditions.append('and')
    conditions.pop()
    return conditions
def parse_where(i, tokens):
    i+=1 #this is now just past the where
    parseFlag = False
    conditions = []
    end_of_where = len(tokens) # Assumption: where clause is the last thing in a query

    if (i >= end_of_where):
        return conditions, i, parseFlag

    where_conditions = tokens[i:end_of_where]
    # print(where_conditions)
    split_list = ["and", "or"] # list of valid splitting tokens
    split_indicies = [] # list of indicies to split conditions on
    for cond_index in range(len(where_conditions)):
        if where_conditions[cond_index] in split_list:
            split_indicies.append(cond_index)

    split_indicies.append(len(where_conditions))
    print(split_indicies)
    # There could also be parenthesis in here which determine Order of Operations
    # Start by assuming there are none
    start_index = 0
    for end_index in split_indicies:
        temp_list = where_conditions[start_index:end_index]
        # print("Temp list: " + str(temp_list))
        if len(temp_list) == 3:
            condition_tuple = tuple(temp_list)
            conditions.append(condition_tuple)
        elif temp_list[0] in split_list: #assuming the first thing is a splitting token
            condition_tuple = (temp_list[0])
            conditions.append(condition_tuple)
            condition_tuple2 = tuple(temp_list[1:])
            conditions.append(condition_tuple2)
        else:
            condition_tuple = tuple(temp_list)
            conditions.append(condition_tuple)
        start_index = end_index

    # print("Where Conditions: " , conditions)
    return conditions

def parse_set(sql):
    new_values = []
    if "set " not in sql:
        return new_values
    next_keyword = next_tag(sql, "set")
    reg = "set (.+)" + next_keyword
    set_statement = re.compile(reg).findall(sql)[0]
    set_statement = set_statement.split(", ")
    for statement in set_statement:
        if "=" in statement:
            compare_keyword = "="
        elif "!=" in statement:
            compare_keyword = "!="
        elif "<" in statement:
            compare_keyword = "<"
        elif ">" in statement:
            compare_keyword = ">"
        elif "<=" in statement:
            compare_keyword = "<="
        elif ">=" in statement:
            compare_keyword = ">="
        reg = "^(.+)\s*" + compare_keyword
        attribute = re.compile(reg).findall(statement)[0]
        attribute = rm_str_space(attribute)
        reg = compare_keyword + "\s*(.+)$"
        value = re.compile(reg).findall(statement)[0]
        value = rm_str_space(value)
        new_values.append(tuple([attribute, compare_keyword, value]))
    return new_values


def parse_select(sql):
    reg = "select (.+) from"
    select = re.compile(reg).findall(sql)[0]
    attributes = select.split(", ")
    return attributes

def parse_from(sql):
    tables = []
    next_keyword = next_tag(sql, "from")
    reg = "from (.+) " + next_keyword
    from_statement = re.compile(reg).findall(sql)[0]
    from_statement = from_statement.split(", ")
    if len(from_statement) == 1:
        tables.append(from_statement)
    else:
        for statement in from_statement:
            statement = statement.split(" ")
            table_alias = (statement[0], statement[1])
            tables.append(table_alias)
    return tables

def set_attributes(attributes, attribute_names):
        for attribute in attribute_names:
            attributes.append(attribute)
        return attributes

def print_result(result):
    output_list = []
    for x in range(0,len(result)):
        output_list.append(result[x])

    for item in output_list:
        print('{0: <10}'.format(item), end= " ")

    print("")


if __name__ == "__main__":
    index = 0
    attrnames = []
    primary = []
    sql = "select * from employee"
    dbmanager = Dbmanager()

    sql = sql.replace(';', '')
    #while sql.find("'") != -1:
        #sql = sql.replace("'", "")
    while sql.find('\t') != -1:
        sql = sql.replace("\t", " ")
    while sql.find('\n') != -1:
        sql = sql.replace("\n", " ")

    sql_tokens = sql.split(" ")
    sql_tokens[:] = [token.lower() for token in sql_tokens]
    #create_database(0, sql_tokens, dbmanager)
    #reg = "\((.*)\)"
    sql = ' '.join(sql_tokens)


    attributes = parse_select(sql)
    print(attributes)