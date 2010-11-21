""" Main sqlite3_diff tools.
"""
import sqlite3_diff.utils as utils
import difflib

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

def diff_table(tbl_name, db1, db2):
    indexes = utils.indexed_column_sets(db1, tbl_name)
    if not indexes is None:
        return __diff_on_index(indexes[0], tbl_name, db1, db2)
    return None

def __diff_on_index(idx, tbl_name, db1, db2):
    rows1 = db1.cursor().execute("SELECT " + ",".join(idx) + ", * FROM " + tbl_name + " ORDER BY " + ",".join(idx) + ";")
    rows2 = db2.cursor().execute("SELECT " + ",".join(idx) + ", * FROM " + tbl_name + " ORDER BY " + ",".join(idx) + ";")

    def get_index(row):
        if row is None:
            return None
        return row[0:len(idx)]

    def get_row(row):
        if row is None:
            return None
        return row[len(idx):]

    def get_next_row_no_error(r):
        try:
            return r.next()
        except StopIteration:
            return None

    def get_next_row(rl1, rl2):
        l = None
        r = None
        count = 0
        try:
            l = rows1.next()
        except StopIteration:
            count += 1
        try:
            r = rows2.next()
        except StopIteration:
            count += 1
        if count == 2:
            raise StopIteration("All rows empty")
        return (l, r)

    def compare_index(l,r):
        for i in range(len(l)):
            if l[i] != r[i]:
                return l[i] < r[i]

    try:
        result = []

        left, right = get_next_row(rows1, rows2)

        while True:
            if (left != right):
                if (left is None or right is None): # If we've reached the end, and one table has rows left
                    result.append((get_row(left), get_row(right)))
                    break

                lidx, ridx = get_index(left), get_index(right)
                if (lidx == ridx): # If ID is the same, but body is not
                    result.append( (get_row(left), get_row(right)) )
                    break

                if (lidx != ridx): # If indexes are not the same
                    if compare_index(lidx, ridx):
                        result.append((get_row(left), None))
                        left = get_next_row_no_error(rows1)
                    else:
                        result.append((None, get_row(right)))
                        right = get_next_row_no_error(rows2)

            left, right = get_next_row(rows1, rows2)
    except StopIteration:
        return result
    return result


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
