""" Main sqlite3_diff tools.
"""
import sqlite3_diff.utils as utils

def table_name_diff(cur1, cur2):
    names1 = utils.table_names(cur1)
    names2 = utils.table_names(cur2)
    diff1 = names1 - names2
    diff2 = names2 - names1
    if len(diff1) == 0 and len(diff2) == 0:
        return False
    else:
        return (list(diff1), list(diff2))

def table_header_diff(db1, db2, name):
    tbl1, ind1 = utils.table_definition(db1.cursor(), name)
    tbl2, ind2 = utils.table_definition(db2.cursor(), name)
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
    names1 = utils.table_names(cur1)
    names2 = utils.table_names(cur2)
    return set(names1).intersection(set(names2))

def table_column_diff(cur1, cur2):
    names = shared_tables(cur1, cur2)
    diffs = []
    for n in names:
        col1 = utils.sqlite_master_table_def(cur1, n)
        col2 = utils.sqlite_master_table_def(cur2, n)
        if col1 != col2:
            diffs.append( (col1, col2) )
    if len(diffs) > 0:
        return diffs
    else:
        return False
