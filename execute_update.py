# coding=utf-8
from utils import *
import numpy as np
from dbmanager import *
from table import *
from database import *

def update(tokens, dbmanager):
    if dbmanager.current_db == None:
        print("Error: No database selected")
        return dbmanager
    else:
        # get the table name
        table_name = tokens[1]
        current_db = dbmanager.get_current_db()
        if not current_db.relation_exists(table_name):
            print("Error: Relation not exists")
            return dbmanager
        else:
            if tokens[2] != "set":
                print("Error: Syntax error")
                return dbmanager
            else:
                sql = ' '.join(tokens)
                # column_and_value looks like [('emp_num', '=', '1'), ('emp_name', '=', 'jon')]
                column_and_values = parse_set(sql)
                # conditions looks like [('emp_num', '=', '1'), 'and', ('emp_name', '=', 'jon')]
                conditions = parse_where(sql)
                table = current_db.get_relation(table_name)
                columns = []
                values = []
                # get the columns and values that should be set
                for column_and_value in column_and_values:
                    columns.append(column_and_value[0])
                    values.append(column_and_value[2])

                attributes = table.attributes
                row_number = table.row_number
                col_number = table.col_number

                column_indexes = []
                for column in columns:
                    column_indexes.append(attributes.index(column))

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
                    row_nums_matched = []
                    for x in range(1, row_number):
                        row_values.append((x, np.take(table.storage, x * col_number + attribute_index).decode('utf-8')))

                    for row_value in range(0, len(row_values)):
                        # get the row numbers for each operation of the value
                        if conditions[condition_index][1] == "=":
                            if row_values[row_value][1] == condition_value:
                                row_nums_matched.append(row_values[row_value][0])
                        elif conditions[condition_index][1] == "!=":
                            if row_values[row_value][1] != condition_value:
                                row_nums_matched.append(row_values[row_value][0])
                        elif conditions[condition_index][1] == "<":
                            if int(row_values[row_value][1]) < int(condition_value):
                                row_nums_matched.append(row_values[row_value][0])
                        elif conditions[condition_index][1] == ">":
                            if int(row_values[row_value][1]) > int(condition_value):
                                row_nums_matched.append(row_values[row_value][0])
                        elif conditions[condition_index][1] == "<=":
                            if int(row_values[row_value][1]) <= int(condition_value):
                                row_nums_matched.append(row_values[row_value][0])
                        elif conditions[condition_index][1] == ">=":
                            if int(row_values[row_value][1]) >= int(condition_value):
                                row_nums_matched.append(row_values[row_value][0])

                    match_rows.append(row_nums_matched)

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
                            print("Error: cannot update because no tuple meets the criteria.")
                            return dbmanager
                        else:
                            for row in intersect_rows:
                                for x in range(0, len(column_indexes)):
                                    np.put(table.storage, row * col_number + column_indexes[x], values[x])
                            print("Updated table %s successfully" %table_name.upper())
                    condition_index += 2
                return dbmanager