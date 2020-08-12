from queryfile import QueryFile
from connect import DBConnect
from varianti import VariantM
import sys
from collections import Counter

def getvars(line,curs,build):
    pos,tot = line.split("\t")
#    pos = int(pos)
    q = 'SELECT * FROM positions WHERE pos = %s AND build = %s'
    curs.execute(q,(pos,build))
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

def mergeids(chose,dups,curs,conn):
    print()
    main = VariantM(curs,chose['id'],chose['pos'],chose['build'],chose['datasource'])
    for dupd in dups:
        print("merging alt %s with main %s" % (dupd['id'],chose['id']))
        try:
            main.snpid_swapin(dupd['id'],dupd['datasource'])
        except NotMerged as nm:
            print("not-merged error: ",nm) # for logging
            conn.rollback()
        else:
            conn.commit()

def main():

    build = '37'
    vals = (build,build)
    qsamepos = ("select pos,count(id) from positions where build = %s group by pos having count(id) > 1 and pos <> 0 and pos not in "
            "( "
                "select p1.pos from positions p1, positions p2  "
                "where "
                "p1.build = p2.build "
                "and "
                "p1.id = p2.id "
                "and "
                "p1.pos <> p2.pos "
                "and "
                "p1.pos <> 0 "
                "and "
                "p2.pos <> 0 "
                "and  "
                "p1.build = %s "
            ") "
            "order by count(id) desc limit 3" # REMOVE LIMIT
            )
    fname = 'samepos.txt'
    qf = QueryFile(fname,qsamepos,vals)
    conn = DBConnect('chip_comp')
    curs = conn.getCursor(dic=True)
    cursm = conn.getCursor()
    for line in qf.read():
        try:
            posdups = getvars(line,curs,build)
            whichind = choose(posdups)
            whichone = posdups[whichind]
            whichdups = [d for i,d in enumerate(posdups) if i != whichind]
            mergeids(whichone,whichdups,cursm,conn)
        except Exception as e:
            conn.rollback()
            conn.close()
            print("error at merging step ",line,sys.exc_info()[0], e)
            raise
#        else:
#            conn.commit() # already done after each sub loop in mergeids
            
    conn.close()


if __name__ == '__main__':
    main()
#variant = VariantI(curs,dic,new_ds,build)
