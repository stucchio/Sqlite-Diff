#!/usr/local/bin/python

import sqlite3

def __table_names(cur):
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    return set(n[0] for n in cur)

def table_name_diff(cur1, cur2):
    names1 = __table_names(cur1)
    names2 = __table_names(cur2)
    diff1 = names1 - names2
    diff2 = names2 - names1
    if len(diff1) == 0 and len(diff2) == 0:
        return False
    else:
        return (list(diff1), list(diff2))


