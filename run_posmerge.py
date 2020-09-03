import time
import datetime
import logging
from queryfile import QueryFile
from connect import DBConnect
from varianti import VariantM
from varianti import NotMerged
import sys
from collections import Counter

def getvars(line,curs,build):
    pos,chrm,tot = line.split("\t")
    q = 'SELECT * FROM positions WHERE pos = %s AND build = %s AND chr = %s'
    curs.execute(q,(pos,build,chrm))
    allpos = curs.fetchall()
    return allpos

def choose(posdups):
    useind = 0
    ids = []
    ds = []
    for d in posdups:
        ds.append(d['datasource'])
        ids.append(d['id'])
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
    main = VariantM(curs,chose['id'],chose['pos'],chose['build'],chose['datasource'],chose['chr'])
    for dupd in dups:
        #print("merging alt %s with main %s" % (dupd['id'],chose['id']))
        try:
            main.snpid_swapin(dupd['id'],dupd['datasource'])
        except NotMerged as nm:
            statement = "not-merged error: " + str(nm)
            logging.warning(statement)
            conn.rollback()
        else:
            conn.commit()

def main():

    db = 'chip_comp'
    build = '38'
    vals = (build,build)
    # for a build, find positions that have multiple entries. filter out positions used by ids that occur multiple times but with different positions. 
    qsamepos = ("select pos,chr,count(id) from positions where build = %s group by pos,chr having count(id) > 1 and pos <> 0 and pos not in "
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
            "order by count(id) desc" # limit 3" # REMOVE LIMIT
            )
    fname = 'samepos.txt'
    qf = QueryFile(fname,qsamepos,vals,db)
    rc = qf.row_count
    fvper = int(0.05 * rc)
    logline = fvper
    logfile = datetime.datetime.now().strftime("pmerge_%a_%d%b_%I%p.log")
    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info('run_posmerge.py: created %s with %s rows, using db %s, merging on build %s',fname,rc,db,build)
    start = time.time()
    count = 0
    conn = DBConnect(db)
    curs = conn.getCursor(dic=True)
    cursm = conn.getCursor()
    for line in qf.read():
        try:
            posdups = getvars(line,curs,build)
            whichind = choose(posdups)
            whichone = posdups[whichind]
            whichdups = [d for i,d in enumerate(posdups) if i != whichind]
            mergeids(whichone,whichdups,cursm,conn)
            count += 1
            if logline == count:
                now = int(time.time() - start)
                logging.info("approximately %.2f%% parsed after %s seconds, %s positions, line: %s" % ((count/rc*100),now,count,line))
                logline += fvper
        except Exception as e:
            conn.rollback()
            conn.close()
            statement = "error at merging step for line " + line + str(sys.exc_info()[0]) + str(e)
            logging.error(statement)
            raise
        #should prob add an else: commit
    now = int(time.time() - start)
    logging.info('Finished after %s seconds (%s rows)' % (now,rc))
    conn.close()


if __name__ == '__main__':
    main()
