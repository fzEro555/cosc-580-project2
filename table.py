import numpy as np
class Table:
    def __init__(self, rows, cols):
        self.name = ""
        self.primary_key = []
        self.storage = np.chararray((rows, cols), itemsize=20)
        self.row_number = rows
        self.col_number = cols
        # index looks like ('in', ['stu_num', 'stu_name'])
        self.indexes = []
        self.attributes = []

    def set_name(self, table_name):
        self.name = table_name.upper()

    def set_primary_key(self, pk):
        for attribute in pk:
            self.primary_key.append(attribute)

    def set_attributes(self, attribute_names):
        for attribute in attribute_names:
            self.attributes.append(attribute)

    def set_row_number(self, row_num):
        self.row_number = row_num

    def set_col_number(self, col_num):
        self.col_number = col_num

    def column_exists(self, attribute):
        return attribute in self.attributes

    def add_index(self, index_name, attributes):
        self.indexes.append((index_name, attributes))

    def index_exists(self, index_name):
        for index in self.indexes:
            if index_name == index[0]:
                return True
        else:
            return False

