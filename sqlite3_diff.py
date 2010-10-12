#!/usr/local/bin/python

import sqlite3

def table_name_diff(cur1, cur2):
    names1 = table_names(cur1)
    names2 = table_names(cur2)
    diff1 = names1 - names2
    diff2 = names2 - names1
    if len(diff1) == 0 and len(diff2) == 0:
        return False
    else:
        return (list(diff1), list(diff2))

def row_count(cur, table_name):
    result = cur.execute("SELECT Count(*) FROM " + table_name + ";" )
    return result.next()[0]

def format_table_diff(name1, name2, db1, db2):
    result = ""
    tnd = table_name_diff(db1.cursor(), db2.cursor())
    if tnd: # First print tables which exist in one but not the other
        for d in tnd[1]:
            kind, nm, tbl_nm, ind, sql = table_columns(db2.cursor(), d)
            result += name2+ "," + str(ind) + "\n"
            result += "> " + sql + "\n"
            result += "> " + str(row_count(db2.cursor(), d)) + " rows\n"
        for d in tnd[0]:
            kind, nm, tbl_nm, ind, sql = table_columns(db1.cursor(), d)
            result += name1 + "," + str(ind) + "\n"
            result += "< " + sql + "\n"
            result += "< " + str(row_count(db1.cursor(), d)) + " rows\n"
    tcd = table_column_diff(db1.cursor(), db2.cursor())
    if tcd:
        for old, new in tcd:
            oldkind, oldn, oldn2, oldind, oldsql = old
            newkind, newn, newn2, newind, newsql = new
            result += name2 + "," + str(newind) + "\n"
            result += "> " + newsql + "\n"
            result += "> " + str(row_count(db2.cursor(), oldn)) + " rows\n"
            result += "---\n"
            result += "< " + oldsql + "\n"
            result += "< " + str(row_count(db1.cursor(), newn)) + " rows\n"
    return result

def table_diff(db1, db2, name):
    tbl1, ind1 = table_definition(db1.cursor(), name)
    tbl2, ind2 = table_definition(db2.cursor(), name)
    table_diff = False
    if (tbl1 != tbl2):
        table_diff = (tbl1, tbl2)
    index_diff = []
    #First compute diffs of indices held in common
    ind1_map = dict( [ (i[1], i) for i in ind1] )
    ind2_map = dict( [ (i[1], i) for i in ind2] )
    for nm in set(ind1_map.keys()).intersection(set(ind2_map.keys())):
        if ind1_map[nm] != ind2_map[nm]:
            index_diff.append( ( ind1_map[nm], ind2_map[nm]) )
    #Now compute indices held by one but not the other
    for nm in set(ind1_map.keys()) - set(ind2_map.keys()):
        index_diff.append( (ind1_map[nm], None) )
    for nm in set(ind2_map.keys()) - set(ind1_map.keys()):
        index_diff.append( (ind2_map[nm], None) )

    if (not table_diff) and (len(index_diff) == 0):
        return False
    else:
        return (table_diff, index_diff)


def table_column_diff(cur1, cur2):
    exclude_tables = []
    tnd = table_name_diff(cur1, cur2)
    if tnd:
        exclude_tables += tnd[0]
        exclude_tables += tnd[1]
    names = table_names(cur1) - set(exclude_tables)
    diffs = []
    for n in names:
        col1 = table_columns(cur1, n)
        col2 = table_columns(cur2, n)
        if col1 != col2:
            diffs.append( (col1, col2) )
    if len(diffs) > 0:
        return diffs
    else:
        return False

def table_definition(cur, tbl_name):
    tbl = cur.execute("SELECT * FROM sqlite_master WHERE type='table' AND name= ? ORDER BY name;", (tbl_name, )).next()
    indexes = list(cur.execute("SELECT * FROM sqlite_master WHERE type='index' AND tbl_name= ? ORDER BY name;", (tbl_name, )))
    return (tbl, indexes)

def table_columns(cur, name):
    cur.execute("SELECT * FROM sqlite_master WHERE type='table' AND name= ? ;", (name, ))
    return cur.next()

def table_names(cur):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    return set(n[0] for n in cur)
