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

def format_table_diff(db1, db2):
    result = ""
    tnd = table_name_diff(db1.cursor(), db2.cursor())
    if tnd: # First print tables which exist in one but not the other
        for d in tnd[1]:
            kind, nm, tbl_nm, ind, sql = table_columns(db2.cursor(), d)
            result += "Table("+ tbl_nm + ")," + str(ind) + "\n"
            result += "> " + sql + "\n"
            result += "> " + str(row_count(db2.cursor(), d)) + " rows\n"
        for d in tnd[0]:
            kind, nm, tbl_nm, ind, sql = table_columns(db1.cursor(), d)
            result += "Table("+ tbl_nm + ")," + str(ind) + "\n"
            result += "< " + sql + "\n"
            result += "< " + str(row_count(db1.cursor(), d)) + " rows\n"
    #Now print tables which differ
    shared = shared_tables(db1.cursor(), db2.cursor())
    for n in shared:
        result += format_one_table_diff(table_diff(db1, db2, n), db1, db2)
    return result

def format_one_table_diff(diff, db1, db2):
    """Assumes we are given output of table_diff function."""
    if not diff:
        return ""
    result = ""
    #First display if one table has differing headers
    if diff[0]:
        oldkind, oldnm, old_tbl_nm, oldind, oldsql = diff[0][0]
        newkind, newnm, new_tbl_nm, newind, newsql = diff[0][1]
        result += "Table(" + new_tbl_nm + ")," + str(newind) + "\n"
        result += "> " + newsql + "\n"
        result += "> " + str(row_count(db2.cursor(), new_tbl_nm)) + " rows\n"
        result += "---\n"
        #result += "Table(" + old_tbl_nm + ")," + str(oldind) + "\n"
        result += "< " + oldsql + "\n"
        result += "< " + str(row_count(db1.cursor(), new_tbl_nm)) + " rows\n"
    #Then display differing indexes
    if diff[1]:
        for old, new in diff[1]: # One of these variables stores the table_name
            table_name = None
            rootpage = None
            if old:
                table_name = old[2]
                rootpage = old[3]
            if new:
                table_name = new[2]
                rootpage = new[3]
            result += "Table("+table_name+"),"+str(rootpage) + "\n"
            if new:
                result += "> " + str(new[4]) + "\n"
            if old and new:
                result += "---\n"
            if old:
                result += "< " + str(old[4]) + "\n"
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
        index_diff.append( (None, ind2_map[nm]) )

    if (not table_diff) and (len(index_diff) == 0):
        return False
    else:
        return (table_diff, index_diff)

def shared_tables(cur1, cur2):
    names1 = table_names(cur1)
    names2 = table_names(cur2)
    return set(names1).intersection(set(names2))

def table_column_diff(cur1, cur2):
    names = shared_tables(cur1, cur2)
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
