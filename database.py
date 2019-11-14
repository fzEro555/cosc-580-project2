# coding=utf-8

class Database:
    def __init__(self, name):
        self.name = name
        self.relations = []
        self.relation_num = 0

    def add_relation(self, relation):
        self.relations.append(relation)
        self.relation_num += 1

    def get_relation(self, relation_name):
        for relation in self.relations:
            if relation.name == relation_name.upper():
                return relation

    def relation_exists(self, relation_name):
        for relation in self.relations:
            if relation.name == relation_name.upper():
                return True
        return False

    def rm_relation(self, relation):
        if self.relation_exists(relation.name):
            self.relations.remove(relation)
            self.relation_num -= 1