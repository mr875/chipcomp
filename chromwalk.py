from resolvef import ResolveF
from connect import DBConnect
from queryfile import QueryFile
import time
import datetime
import logging

def walk(qfit,curs):
    pass

def itls(qf):
    for ls in qf.readls():
        yield ls

def main():

    db = 'chip_comp'
    q = "SELECT DISTINCT chr FROM positions"
    conn = DBConnect(db)
    curs = conn.getCursor()
    try:
        curs.execute(q)
        chrs = curs.fetchall()
    except Exception as e:
        print("closing")
        conn.close()
        raise

    chrs = [st[0] for st in chrs]
    for chrm in chrs[0]:
        fname = chrm + ".txt"
        q = "SELECT id,pos FROM positions WHERE chr = %s ORDER BY pos ASC LIMIT 100" # remove limit after testing
        vals = (chrm,)
        qf = QueryFile(fname,q,vals,db)
        qfit = itls(qf)
        walk(qfit,curs) 
    
    conn.close()

if __name__ == '__main__':
    main()
