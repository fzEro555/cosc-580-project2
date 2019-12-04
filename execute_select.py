# coding=utf-8
from dbmanager import *
from table import *
from database import *
from utils import *
import numpy as np


def select_from(attributes, tables, conditions, dbmanager):
    if dbmanager.current_db == None:
        print("Error: No database selected")
        return

    else:
        current_db = dbmanager.get_current_db()
        results = []
        index_flag = False
        none_index_conditions = []

        for table in tables:
            if type(table) is tuple:
                table_name = table[0]
            else:
                table_name = table

            if not current_db.relation_exists(table_name):
                print("Error: Relation not exists")
                return
            else:
                temp_table = current_db.get_relation(table_name)
                # look the indexes of a table to judge whether index exists in conditions
                if len(temp_table.indexes) > 0:
                    for index in temp_table.indexes:
                        for condition in conditions:
                            if condition[0] == index[1]:
                                # no index in condition
                                if len(conditions) == 1:
                                    break;
                                # index in condition, get those none index condition
                                else:
                                    index_flag = True
                                    for temp_condition in conditions:
                                        if temp_condition is not condition and temp_condition != "and" and temp_condition != "or":
                                            none_index_conditions.append(temp_condition)

        # suppose only one join condition for a select query

        if len(tables) == 1:
            if not current_db.relation_exists(tables[0]):
                print("Error: Relation not exists")
                return
            else:
                table = current_db.get_relation(tables[0])
        # select from two tables with joining condition
        elif len(tables) > 1:
            for condition in conditions:
                if "." in condition[0] and "." in condition[2]:
                    join_index = conditions.index(condition)
            join_condition = conditions[join_index]
            conditions.remove(join_condition)
            if "and" in conditions:
                conditions.remove("and")
            if "or" in conditions:
                conditions.remove("or")

            table1_name = tables[0][0]
            table1_alias = tables[0][1]

            join_index = [i for i, x in enumerate(join_condition) if table1_alias + "." in x]
            table1_attribute = join_condition[join_index[0]]
            table1_attribute = table1_attribute.replace(".", "")
            table1_attribute = table1_attribute.replace(table1_alias, "", 1)

            table2_name = tables[1][0]
            table2_alias = tables[1][1]

            join_index = [i for i, x in enumerate(join_condition) if table2_alias + "." in x]
            table2_attribute = join_condition[join_index[0]]
            table2_attribute = table2_attribute.replace(".", "")
            table2_attribute = table2_attribute.replace(table2_alias, "", 1)

            table1 = current_db.get_relation(table1_name)
            table2 = current_db.get_relation(table2_name)

            table1_size = table1.row_number-1
            table2_size = table2.row_number-1

            if int(table1_size/table2_size) > 1000 or int(table2_size/table1_size > 1000):
                table = nested_loop(table1, table2, table1_attribute, table2_attribute)

            else:
                table = merge_join(table1, table2, table1_attribute, table2_attribute)

        table_attributes = table.attributes
        for condition in conditions:
            if condition != "and" and condition != "or":
                if "." in condition[0]:
                    attribute = condition[0].split(".")[1]
                else:
                    attribute = condition[0]
                if not table.column_exists(attribute):
                    print("Error: attribute not exists in table")
        # select some attributes
        if attributes[0] != "*":
            attributes_indexes = []
            for attribute in attributes:
                if "." in attribute:
                    attribute = attribute.split(".")[1]
                else:
                    attribute = attribute
                attributes_indexes.append(table_attributes.index(attribute))

            if not conditions:
                results = select_without_condition(table, attributes_indexes)
            else:
                condition_number = int((len(conditions) + 1) / 2)
                if condition_number == 1:
                    results = select_with_one_condition(table, table_attributes, conditions, attributes_indexes)
                else:
                    results = select_with_multiple_condition(table, table_attributes. conditions, attributes_indexes)
        # select all
        else:
            attributes_indexes = []
            for attribute in table.attributes:
                attributes_indexes.append(table_attributes.index(attribute))
            if not conditions:
                results = select_without_condition(table, attributes_indexes)
            else:
                condition_number = int((len(conditions) + 1) / 2)
                if condition_number == 1:
                    results = select_with_one_condition(table, table_attributes, conditions, attributes_indexes)
                else:
                    results = select_with_multiple_condition(table, table_attributes. conditions, attributes_indexes)
        result_table = Table(len(results), len(results[0]))
        result_table.set_name("temporary_table")

        indexes = []
        for x in range(0, len(results[0])):
            indexes.append(x)

        loc = 0
        for row in results:
            temp_row = np.take(row, indexes)
            for i in range(0, len(temp_row)):
                np.put(result_table.storage, loc, temp_row[i])
                loc += 1

        if index_flag:
            current_db.add_relation(result_table)
            print("Final output:")
            select_from(attributes, "temporary_table", none_index_conditions, dbmanager)
            current_db.rm_relation(result_table)


def print_result(result):
    output = []
    for x in range(0, len(result)):
        output.append(result[x])
    print("| ", end="")
    for i in output:
        print('{0: <15}'.format(i.decode('utf-8')), end=" |")
    print("")


def select_without_condition(table, attributes_indexes):
    results = []
    row = attributes_indexes[:]
    for row_num in range(0, table.row_number):
        for x in range(0, len(attributes_indexes)):
            row[x] = row_num * table.col_number + attributes_indexes[x]
        row_data = np.take(table.storage, row)
        results.append(row_data)
        print_result(row_data)
    return results


def select_with_one_condition(table, table_attributes, conditions, attributes_indexes):
    if "." in conditions[0][0]:
        attribute = conditions[0][0].split(".")[1]
    else:
        attribute = conditions[0][0]
    value = conditions[0][2]
    attribute_index = table_attributes.index(attribute)
    row_number = table.row_number
    col_number = table.col_number
    row_values = []
    matched_row_number = []

    for x in range(1, row_number):
        row_values.append((x, np.take(table.storage, x * col_number + attribute_index).decode('utf-8')))

    for row_value in range(0, len(row_values)):
        if conditions[0][1] == "=":
            if row_values[row_value][1] == value:
                matched_row_number.append(row_values[row_value][0])
        elif conditions[0][1] == "!=":
            if row_values[row_value][1] != value:
                matched_row_number.append(row_values[row_value][0])
        elif conditions[0][1] == "<":
            if int(row_values[row_value][1]) < int(value):
                matched_row_number.append(row_values[row_value][0])
        elif conditions[0][1] == ">":
            if int(row_values[row_value][1]) > int(value):
                matched_row_number.append(row_values[row_value][0])
        elif conditions[0][1] == ">=":
            if int(row_values[row_value][1]) >= int(value):
                matched_row_number.append(row_values[row_value][0])
        elif conditions[0][1] == "<=":
            if int(row_values[row_value][1]) <= int(value):
                matched_row_number.append(row_values[row_value][0])
    if not matched_row_number:
        print("No tuple meets the criteria")
        return []
    else:
        results = []
        attributes = []
        for i in attributes_indexes:
            attributes.append(np.take(table.storage, i))
        print_result(attributes)
        row = attributes_indexes[:]
        for row_num in matched_row_number:
            for x in range(0, len(attributes_indexes)):
                row[x] = row_num * col_number + attributes_indexes[x]
            row_data = np.take(table.storage, row)
            results.append(row_data)
            print_result(row_data)
        return results


def select_with_multiple_condition(table, table_attributes, conditions, attributes_indexes):
    condition_number = int((len(conditions) + 1) / 2)
    condition_index = 0

    row_number = table.row_number
    col_number = table.col_number

    results = []

    # a list contains several lists, every list contains the matching row numbers for each condition
    match_rows = []

    # a list contains the finally matching row numbers
    intersect_rows = []

    # an iterator used to iterate match_rows to get the intersection
    match_rows_iterator = 1

    for i in range(0, condition_number):

        condition_attribute = conditions[condition_index][0]
        condition_value = conditions[condition_index][2]

        attribute_index = table_attributes.index(condition_attribute)
        # a list used to contain all values of every row of the attribute in condition
        row_values = []
        matched_row_number = []
        for x in range(1, row_number):
            row_values.append((x, np.take(table.storage, x * col_number + attribute_index).decode('utf-8')))

        for row_value in range(0, len(row_values)):
            # get the row numbers for each operation of the value
            if conditions[condition_index][1] == "=":
                if row_values[row_value][1] == condition_value:
                    matched_row_number.append(row_values[row_value][0])
            elif conditions[condition_index][1] == "!=":
                if row_values[row_value][1] != condition_value:
                    matched_row_number.append(row_values[row_value][0])
            elif conditions[condition_index][1] == "<":
                if int(row_values[row_value][1]) < int(condition_value):
                    matched_row_number.append(row_values[row_value][0])
            elif conditions[condition_index][1] == ">":
                if int(row_values[row_value][1]) > int(condition_value):
                    matched_row_number.append(row_values[row_value][0])
            elif conditions[condition_index][1] == "<=":
                if int(row_values[row_value][1]) <= int(condition_value):
                    matched_row_number.append(row_values[row_value][0])
            elif conditions[condition_index][1] == ">=":
                if int(row_values[row_value][1]) >= int(condition_value):
                    matched_row_number.append(row_values[row_value][0])

        match_rows.append(matched_row_number)

        if len(match_rows) > 1:
            if conditions[condition_index - 1] == "and":
                intersect_rows = list(set(match_rows[0]) & set(match_rows[match_rows_iterator]))
                match_rows[0] = intersect_rows[:]
                match_rows_iterator += 1
            elif conditions[condition_index - 1] == "or":
                intersect_rows = list(set(match_rows[0]) | set(match_rows[match_rows_iterator]))
                match_rows[0] = intersect_rows[:]
                match_rows_iterator += 1

        elif len(match_rows) == 1:
            intersect_rows = match_rows[0]

        # after iterating all conditions
        if i == condition_number - 1:
            if not intersect_rows:
                print("No tuple meets the criteria")
                return results
            else:
                row = attributes_indexes[:]
                attributes = []
                for i in attributes_indexes:
                    attributes.append(np.take(table.storage, i))
                print_result(attributes)
                for row_num in matched_row_number:
                    for x in range(0, len(attributes_indexes)):
                        row[x] = row_num * col_number + attributes_indexes[x]
                    row_data = np.take(table.storage, row)
                    results.append(row_data)
                    print_result(row_data)
        condition_index += 2
    return results

def merge_join(table1, table2, attribute1, attribute2):
    # sort the two relations based on the joining attribute
    try:
        temp_table1 = table1.storage.tolist()
        temp_table2 = table2.storage.tolist()

        attribute1_index = table1.attributes.index(attribute1)
        attribute2_index = table2.attributes.index(attribute2)

        # check whether the joining attribute is integer

        temp_table1 = list_2d_encode_utf8(temp_table1)
        temp_table2 = list_2d_encode_utf8(temp_table2)

        for row in temp_table1:
            if temp_table1.index(row) != 0:
                row[attribute1_index] = int(row[attribute1_index])

        for row in temp_table2:
            if temp_table2.index(row) != 0:
                row[attribute2_index] = int(row[attribute2_index])

        temp_table1.pop(0)
        temp_table2.pop(0)

        sorted_table1 = sorted(temp_table1, key=(lambda x: x[attribute1_index]))
        sorted_table2 = sorted(temp_table2, key=(lambda x: x[attribute2_index]))

    except:
        temp_table1 = table1.storage.tolist()
        temp_table2 = table2.storage.tolist()

        attribute1_index = table1.attributes.index(attribute1)
        attribute2_index = table2.attributes.index(attribute2)

        temp_table1 = list_2d_encode_utf8(temp_table1)
        temp_table2 = list_2d_encode_utf8(temp_table2)


        temp_table1.pop(0)
        temp_table2.pop(0)

        sorted_table1 = sorted(temp_table1, key=(lambda x: x[attribute1_index]))
        sorted_table2 = sorted(temp_table2, key=(lambda x: x[attribute2_index]))


    # match rows
    i = 0
    j = 0
    row_counter = 0
    table1_row_index = []
    table2_row_index = []
    while i < len(sorted_table1) and j < len(sorted_table2):
        if sorted_table1[i][attribute1_index] == sorted_table2[j][attribute2_index]:
            table1_row_index.append(i)
            table2_row_index.append(j)
            i += 1
            j += 1
            row_counter += 1

        elif sorted_table1[i][attribute1_index] < sorted_table2[j][attribute2_index]:
            i += 1

        elif sorted_table1[i][attribute1_index] > sorted_table2[j][attribute2_index]:
            j += 1

    result_table = Table(row_counter + 1, len(sorted_table1[0]) + len(sorted_table2[0]) - 1)
    result_table.set_attributes(table1.attributes)
    result_table.set_attributes(table2.attributes)


    for x in range(0, result_table.col_number):
        result_table.storage[0,x] = result_table.attributes[x]

    for x in range(0,row_counter):
        for y in range(0,len(table1.attributes)):
            result_table.storage[x+1][y] = sorted_table1[table1_row_index[x]][y]
        for z in range(0,len(table2.attributes)):
            if z < attribute2_index:
                result_table.storage[x+1][len(table1.attributes) + z] = sorted_table2[table2_row_index[x]][z]
            if z == attribute2_index:
                continue
            if z > attribute2_index:
                result_table.storage[x+1][len(table1.attributes) + z-1] = sorted_table2[table2_row_index[x]][z]

    return result_table

def list_2d_encode_utf8(list_2d):
    result_list = []
    for row in list_2d:
        temp = []
        for i in range(0, len(row)):
            if is_unicode_char(row[i]):
                temp.append(row[i].decode('utf-8'))
            else:
                temp.append(row[i])

        result_list.append(temp)
    return result_list

def is_unicode_char (char):
    return isinstance(char, bytes)

def nested_loop(table1, table2, attribute1, attribute2):
    # sort the two relations based on the joining attribute
    try:
        temp_table1 = table1.storage.tolist()
        temp_table2 = table2.storage.tolist()

        attribute1_index = table1.attributes.index(attribute1)
        attribute2_index = table2.attributes.index(attribute2)

        # check whether the joining attribute is integer

        temp_table1 = list_2d_encode_utf8(temp_table1)
        temp_table2 = list_2d_encode_utf8(temp_table2)

        for row in temp_table1:
            if temp_table1.index(row) != 0:
                row[attribute1_index] = int(row[attribute1_index])

        for row in temp_table2:
            if temp_table2.index(row) != 0:
                row[attribute2_index] = int(row[attribute2_index])

        temp_table1.pop(0)
        temp_table2.pop(0)

    except:
        temp_table1 = table1.storage.tolist()
        temp_table2 = table2.storage.tolist()

        attribute1_index = table1.attributes.index(attribute1)
        attribute2_index = table2.attributes.index(attribute2)

        temp_table1 = list_2d_encode_utf8(temp_table1)
        temp_table2 = list_2d_encode_utf8(temp_table2)

        temp_table1.pop(0)
        temp_table2.pop(0)


    # match rows
    row_counter = 0
    table1_row_index = []
    table2_row_index = []

    for i in range(0, len(temp_table1)):
        for j in range(0,len(temp_table2)):
            if temp_table1[i][attribute1_index] == temp_table2[j][attribute2_index]:
                table1_row_index.append(i)
                table2_row_index.append(j)
                row_counter += 1

    result_table = Table(row_counter + 1, len(temp_table1[0]) + len(temp_table2[0]) - 1)
    result_table.set_attributes(table1.attributes)
    result_table.set_attributes(table2.attributes)

    for x in range(0, result_table.col_number):
        result_table.storage[0, x] = result_table.attributes[x]

    for x in range(0, row_counter):
        for y in range(0, len(table1.attributes)):
            result_table.storage[x + 1][y] = temp_table1[table1_row_index[x]][y]
        for z in range(0, len(table2.attributes)):
            if z < attribute2_index:
                result_table.storage[x + 1][len(table1.attributes) + z] = temp_table2[table2_row_index[x]][z]
            if z == attribute2_index:
                continue
            if z > attribute2_index:
                result_table.storage[x + 1][len(table1.attributes) + z - 1] = temp_table2[table2_row_index[x]][z]
    return result_table