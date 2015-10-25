#!/usr/bin/env python

import re
import sys
import sqlite3

SQL=False

matcher=re.compile("^CubicVT\\[(\\d+),(\\d+)\\] := [^|]+ \\| ([^>]+)>;$")

def process_sql(m):
    conn.execute("INSERT INTO cvt (n, k, edges) VALUES (?, ?, ?);",
                 (m.group(1),m.group(2),m.group(3)))
def process_py(m):
    print "["+m.group(3)[1:-1].replace("{","(").replace("}",")")+"]"
def process(l,num):
    m=matcher.match(l)
    if m is None: raise RuntimeError("malformed input line %d"%(num))
    if SQL:
        process_sql(m)
    else:
        process_py(m)

line=""
cnt=0

if SQL:
    conn=sqlite3.connect(sys.argv[2])

    conn.execute("PRAGMA journal_mode = OFF;")
    conn.execute("CREATE TABLE cvt (id INTEGER PRIMARY KEY AUTOINCREMENT, n INTEGER, k INTEGER, edges TEXT);")
    conn.execute("CREATE INDEX n_idx ON cvt(n);")
    conn.execute("CREATE INDEX k_idx ON cvt(k);")

for l_ in open(sys.argv[1]):
    l=l_.strip()
    if l!="":
        line+=l
        continue
    process(line,cnt)
    cnt+=1
    line=""
if line!="":
    process(line,cnt)

if SQL:
    conn.execute("VACUUM;")
    conn.commit()
    conn.close()

