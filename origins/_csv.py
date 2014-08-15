import csv


# http://stackoverflow.com/a/6187936/407954
class UnicodeCsvReader(object):
    def __init__(self, f, encoding='utf-8', **kwargs):
        self.csv_reader = csv.reader(f, **kwargs)
        self.encoding = encoding

    def __iter__(self):
        return self

    def __next__(self):
        # read and split the csv row into fields
        row = next(self.csv_reader)
        # now decode
        return [str(cell, self.encoding) if isinstance(cell, bytes) else cell
                for cell in row]

    next = __next__

    @property
    def line_num(self):
        return self.csv_reader.line_num


class UnicodeDictReader(csv.DictReader):
    def __init__(self, f, encoding='utf-8', fieldnames=None, **kwds):
        csv.DictReader.__init__(self, f, fieldnames=fieldnames, **kwds)
        self.reader = UnicodeCsvReader(f, encoding=encoding, **kwds)
