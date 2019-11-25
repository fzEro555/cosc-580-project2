class Table:
    def __init__(self, name, pk, fk):
        self.name = name
        self.primary_key = pk
        self.foreign_key = fk