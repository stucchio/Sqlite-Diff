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

def primary_key(cur, table_name):
    """Return a list of columns in the primary key.

    Implementation note: I *assume* that the 6'th column of rows return by the
    PRAGMA TABLE_INFO(...) command is 1 if the column is a primary key, 0 otherwise.

    In some simple experiments, this appears to be the case. But documentation is scant...
    """
    return [c for c in table_columns(cur, table_name) if c[5] == 1]

def sqlite_master_table_def(cur, name):
    """Gets the columns of a table."""
    cur.execute("SELECT * FROM sqlite_master WHERE type='table' AND name= ? ;", (name, ))
    return cur.next()

def table_names(cur):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    return set(n[0] for n in cur)
