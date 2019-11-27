# coding=utf-8
import re
from table import *
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

if __name__ == "__main__":
    index = 0
    attrnames = []
    primary = []
    sql = "CREATE TABLE employees (" \
          "emp_no int(11) NOT NULL," \
          "birth_date date NOT NULL," \
          "first_name varchar(14) NOT NULL," \
          "last_name varchar(16) NOT NULL," \
          "gender enum('M','F') NOT NULL," \
          "hire_date date NOT NULL," \
          "PRIMARY KEY ('emp_no', 'birth_date'));"
    sql = sql.replace(';', '')
    while sql.find("'") != -1:
        sql = sql.replace("'", "")
    while sql.find('\t') != -1:
        sql = sql.replace("\t", " ")
    while sql.find('\n') != -1:
        sql = sql.replace("\n", " ")
    sql_tokens = sql.split(" ")
    sql_tokens[:] = [token.lower() for token in sql_tokens]
    i = 3
    values = ' '.join(sql_tokens[:])
    reg = "\((.*)\)"
    attrstr = re.compile(reg).findall(values)

    #print(attrstr[0])
    attrs = getAttrCons(attrstr[0])
    table_name = sql_tokens[i + 2]
    for attr in attrs:
        reg = "primary\s*key.*\((.*)\)+"
        prikey = re.compile(reg).findall(attr)
        #print(attr)
        if len(prikey) > 0:
            prikey = prikey[0]
            primary = prikey.split(', ')

        else:
            attr = attr.split(' ')
            attrnames.append(attr[0])

    row_number = 1
    col_number = len(attrnames) + 1
    table = Table(row_number, col_number)
    table.set_name(table_name)
    table.set_primary_key(primary)
    table.set_attributes(attrnames)
    table.storage.fill(0)
    print(table.storage)
    np.put(table.storage, index, "key")
    for x in range(1, table.col_number):
        np.put(table.storage, x, attrnames[x - 1])

    print(table.storage)