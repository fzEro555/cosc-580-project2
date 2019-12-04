# coding=utf-8
from utils import *
import numpy as np
from dbmanager import *
from table import *
from database import *

def delete_from(tokens, dbmanager):
    if dbmanager.current_db == None:
        print("Error: No database selected")
        return dbmanager
    else:
        # get the table name
        table_name = tokens[2]
        current_db = dbmanager.get_current_db()
        if not current_db.relation_exists(table_name):
            print("Error: Relation not exists")
            return dbmanager
        else:
            sql = ' '.join(tokens)
            # conditions looks like [('emp_num', '=', '1'), 'and', ('emp_name', '=', 'jon')]
            conditions = parse_where(sql)
            table = current_db.get_relation(table_name)
            if len(conditions) == 0:
                table.storage.resize(1, table.col_number)
                for x in range(0, table.col_number):
                    np.put(table.storage, x, table.attributes[x])
                table.set_row_number(1)
                print("Delete all data from table %s successfully" %table_name.upper())
                return dbmanager
            else:
                attributes = table.attributes
                row_number = table.row_number
                col_number = table.col_number
                condition_number = int((len(conditions) + 1) / 2)
                condition_index = 0

                # a list contains several lists, every list contains the matching row numbers for each condition
                match_rows = []

                # a list contains the finally matching row numbers
                intersect_rows = []

                # an iterator used to iterate match_rows to get the intersection
                match_rows_iterator = 1

                for i in range(0, condition_number):

                    condition_attribute = conditions[condition_index][0]
                    condition_value = conditions[condition_index][2]

                    attribute_index = attributes.index(condition_attribute)
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
                            print("Error: cannot delete because no tuple meets the criteria.")
                            return dbmanager
                        else:
                            table.storage = np.delete(table.storage, intersect_rows, axis=0)
                            table.set_row_number(row_number - len(intersect_rows))
                            print("Delete tuples successfully")
                    condition_index += 2
                return dbmanager