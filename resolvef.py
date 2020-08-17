from connect import DBConnect
import time
import datetime
import logging
from queryfile import QueryFile

class ResolveF:

    def __init__(self,bfile):
        self.bfile = bfile

def getvars(line,curs):
    idf = line.split("\t")[0]
    q = 'SELECT * FROM flank WHERE id = %s'
    curs.execute(q,(idf,))
    allfl = curs.fetchall()
    return allfl

def main():

    db = 'cc2'
    q = "SELECT id,COUNT(flank_seq) FROM flank GROUP BY id HAVING COUNT(flank_seq) > %s ORDER BY COUNT(flank_seq) DESC limit 10" # REMOVE limit after testing
    vals = (2,)
    fname = 'twos.txt'
    qf = QueryFile(fname,q,vals,db)
    rc = qf.row_count
    conn = DBConnect(db)
    curs = conn.getCursor(dic=True)
    for line in qf.read():
        allfl = getvars(line,curs)
        print(allfl)
        print()
    conn.close()


if __name__ == '__main__':
    main()
