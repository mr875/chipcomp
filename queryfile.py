from connect import DBConnect
import sys
from collections import Counter

class QueryFile:

    def __init__(self,bfile,qry,db="chip_comp"):
        self.bfile = bfile
        self.qry = qry
        self.db = db
        self._makeF()   #put back for file generation

    def _makeF(self):
        try:
            f = open(self.bfile,"w")
            conn = DBConnect(self.db)
            curs = conn.getCursor()
            curs.execute(self.qry)
            for row in curs:
                row = [str(i) for i in row]
                f.write("\t".join(row))
                f.write("\n")
        except Exception as e:
            print("error connecting/executing query/writing to file ",sys.exc_info()[0], e)
        finally:
            f.close()
            curs.close()
            conn.close()


    def read(self):
        with open(self.bfile) as f:
            for line in f:
                yield line

def getvars(line,curs):
    pos,tot = line.split("\t")
#    pos = int(pos)
    q = 'SELECT * FROM positions WHERE pos = %s'
    curs.execute(q,(pos,))
    allpos = curs.fetchall()
    return allpos

def choose(posdups):
    useind = 0
    ids = []
    ds = []
    for d in posdups:
        ds.append(d['datasource'])
        ids.append(d['id'])
    #leastds = ds.index(Counter(ds).most_common()[-1][0])
    #print(ds[leastds])
    leastds = Counter(ds).most_common()[-1][0]
    leastds = [i for i,x in enumerate(ds) if x == leastds]
    rs = [i for i,x in enumerate(ids) if "rs" in x]
    common = set(leastds) & set(rs)
    if len(common):
        return common.pop()
    elif len(rs):
        return rs[0]
    else:
        return leastds[0]

def mergeids(chose,dups):
    print()
    print("need to merge these ",dups)
    print("with ",chose)

def main():

    qsamepos = "select pos,count(id) from positions where build = '37' group by pos having count(id) > 1 and pos <> 0 order by count(id) desc limit 15" # REMOVE limit after testing
#    qsamepos = ("select count(*) from positions",())
    fname = 'samepos.txt'
    qf = QueryFile(fname,qsamepos)
    conn = DBConnect('chip_comp')
    curs = conn.getCursor(dic=True)
    for line in qf.read():
        posdups = getvars(line,curs)
        whichind = choose(posdups)
        whichone = posdups[whichind]
        whichdups = [d for i,d in enumerate(posdups) if i != whichind]
        mergeids(whichone,whichdups)

if __name__ == '__main__':
    main()
#variant = VariantI(curs,dic,new_ds,build)
