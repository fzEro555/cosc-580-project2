# coding=utf-8
import re
statement_tag = ["select", "from", "set", "where", "group by", "order by"]


def remove_list_space(list):
    while "" in list:
        list.remove("")
    return list


def next_tag(sql,current_tag):
    index = sql.find(current_tag, 0)
    for tag in statement_tag:
        if sql.find(tag, index+len(current_tag)) != -1:
            return tag
    return ""


def remove_string_space(string):
    list = string.split(" ")
    list = remove_list_space(list)
    result = ''
    for word in list:
        result += word+" "
    return result[0:-1]


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
        attribute = remove_string_space(attribute)
        reg = compare_keyword + "\s*(.+)$"
        value = re.compile(reg).findall(statement)[0]
        value = remove_string_space(value)
        new_values.append(tuple([attribute, compare_keyword, value]))
    return new_values

def parse_where(sql):
    conditions = []
    if "where " not in sql:
        return conditions
    next_keyword = next_tag(sql, "where")
    reg = "where (.+)"+next_keyword
    where = re.compile(reg).findall(sql)[0]
    where_statement = where.split(" and ")
    for and_statement in where_statement:
        and_statement = and_statement.split(" or ")
        for or_statement in and_statement:
            if "=" in or_statement:
                compare_keyword = "="
            elif "!=" in or_statement:
                compare_keyword = "!="
            elif "<" in or_statement:
                compare_keyword = "<"
            elif ">" in or_statement:
                compare_keyword = ">"
            elif "<=" in or_statement:
                compare_keyword = "<="
            elif ">=" in or_statement:
                compare_keyword = ">="
            reg = "^(.+)\s*"+compare_keyword
            attribute = re.compile(reg).findall(or_statement)[0]
            attribute = remove_string_space(attribute)
            reg = compare_keyword+"\s*(.+)$"
            value = re.compile(reg).findall(or_statement)[0]
            value = remove_string_space(value)
            conditions.append(tuple([attribute, compare_keyword, value]))
            conditions.append('or')
        conditions.pop()
        conditions.append('and')
    conditions.pop()
    return conditions

def parse_select(sql):
    attributes = []
    reg = "select (.+) from"
    select_statement = re.compile(reg).findall(sql)[0]
    select_statement = select_statement.split(", ")
    if len(select_statement) == 1:
        attributes.append(select_statement[0])
    else:
        attributes = select_statement
    return attributes

def parse_from(sql):
    tables = []
    next_keyword = next_tag(sql, "from")
    reg = "from (.+)" + next_keyword
    from_statement = re.compile(reg).findall(sql)[0]

    from_statement = from_statement.split(", ")
    for statement in from_statement:
        statement = statement.rstrip()

    # suppose that if there is only one table in the from statement, then no alias
    if len(from_statement) == 1:
        tables.append(from_statement[0].rstrip())
    # suppose that if there is more than one table in the from statement, then must have alias
    elif len(from_statement) > 1:
        for statement in from_statement:
            statement = statement.split(" ")
            table_alias = (statement[0], statement[1].rstrip())
            tables.append(table_alias)
    return tables
