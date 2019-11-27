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

if __name__ == "__main__":
    index = 0
    attrnames = []
    primary = []
    sql = "CREATE DATABASE STUDENT;"
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
    create_database(0, sql_tokens, dbmanager)