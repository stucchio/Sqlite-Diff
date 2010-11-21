#!/usr/local/bin/python

import sqlite3

def row_count(cur, table_name):
    result = cur.execute("SELECT Count(*) FROM " + table_name + ";" )
    return result.next()[0]

def table_definition(cur, tbl_name):
    tbl = cur.execute("SELECT * FROM sqlite_master WHERE type='table' AND name= ? ORDER BY name;", (tbl_name, )).next()
    indexes = list(cur.execute("SELECT * FROM sqlite_master WHERE type != 'table' AND tbl_name= ? ORDER BY name;", (tbl_name, )))
    return (tbl, indexes)

def table_columns(cur, table_name):
    """Return a list of columns in a given table.

    Implementation note: I *assume* that the 6'th column of rows return by the
    PRAGMA TABLE_INFO(...) command is 1 if the column is a primary key, 0 otherwise.

    In some simple experiments, this appears to be the case. But documentation is scant...
    """
    cur.execute("PRAGMA TABLE_INFO(" + table_name + ");")
    return list(cur)

def primary_key(db, table_name):
    """Return a list of columns in the primary key.

    Implementation note: I *assume* that the 6'th column of rows return by the
    PRAGMA TABLE_INFO(...) command is 1 if the column is a primary key, 0 otherwise.

    In some simple experiments, this appears to be the case. But documentation is scant...
    """
    pk_columns = [c[1] for c in table_columns(db.cursor(), table_name) if c[5] == 1]
    if len(pk_columns) == 0:
        return None
    indices = [index_info(db.cursor(), i) for i in unique_index_list(db.cursor(), table_name)]
    resultset = [idx for idx in indices if set(idx) == set(pk_columns)]
    return resultset[0]

def __sort_by_length_alphabetical(left, right):
    if len(left) != len(right):
        if len(left) < len(right):
            return -1
        else:
            return 1
    for i in range(len(left)):
        if left[i] < right[i]:
            return -1
        if left[i] > right[i]:
            return 1
    return 0


def indexed_column_sets(db, table_name):
    """Returns a list of columns or column sets for which an index is available.

    First, it will try to return the name of the primary key (if there is one).

    Then, if no primary key exists, it will return the remaining indices in order of length.
    """
    result = []
    pk = primary_key(db, table_name)
    if not (pk is None):
        result.append(pk)
    #Now add unique indices
    uidx = [index_info(db.cursor(), i) for i in unique_index_list(db.cursor(), table_name)]
    uidx = [idx for idx in uidx if idx != pk] # Remove the primary key

    uidx = sorted(uidx, key=cmp_to_key(__sort_by_length_alphabetical) ) #Sort by length
    result += uidx
    return result

def unique_index_list(cur, table_name):
    cur.execute("PRAGMA INDEX_LIST(" + table_name + ");")
    result = list(cur)
    return [r[1] for r in result if r[2] == 1]

def index_info(cur, idx_name):
    """Given an index name, returns a list of the columns it indexes
    (in the order they appear in the index)."""
    cur.execute("PRAGMA INDEX_INFO(" + idx_name + ");")
    return tuple([c[2] for c in list(cur)])

def sqlite_master_table_def(cur, name):
    """Gets the columns of a table."""
    cur.execute("SELECT * FROM sqlite_master WHERE type='table' AND name= ? ;", (name, ))
    return cur.next()

def table_names(cur):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    return set(n[0] for n in cur)

def cmp_to_key(mycmp):
    'Convert a cmp= function into a key= function'
    class K(object):
        def __init__(self, obj, *args):
            self.obj = obj
        def __lt__(self, other):
            return mycmp(self.obj, other.obj) < 0
        def __gt__(self, other):
            return mycmp(self.obj, other.obj) > 0
        def __eq__(self, other):
            return mycmp(self.obj, other.obj) == 0
        def __le__(self, other):
            return mycmp(self.obj, other.obj) <= 0
        def __ge__(self, other):
            return mycmp(self.obj, other.obj) >= 0
        def __ne__(self, other):
            return mycmp(self.obj, other.obj) != 0
    return K
