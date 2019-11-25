# coding=utf-8
import re

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

if __name__ == "__main__":
    sql = "CREATE TABLE employees ('emp_no' int(11) NOT NULL, " \
          "birth_date date NOT NULL, first_name varchar(14) NOT NULL, " \
          "last_name varchar(16) NOT NULL, gender enum('M','F') NOT NULL, " \
          "hire_date date NOT NULL, PRIMARY KEY (`emp_no`));"
    sql = sql.replace(';', '')
    while sql.find("'") != -1:
        sql = sql.replace("'", "")
    print(sql)
    sql_tokens = sql.split(" ")
    sql_tokens[:] = [token.lower() for token in sql_tokens]
    i = 3
    values = ' '.join(sql_tokens[:])
    reg = "\((.*)\)"
    attrstr = re.compile(reg).findall(values)

    print(attrstr[0])
    attrs = getAttrCons(attrstr[0])

    print(attrs)