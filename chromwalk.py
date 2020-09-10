from resolvef import ResolveF
from connect import DBConnect
from queryfile import QueryFile
import time
import datetime
import logging

class WalkF(ResolveF):

    def __init__(self):
        self.switchdic = {"A":"T","C":"G","G":"C","T":"A"}
        self.switchextra = {"N":"N","R":"Y","Y":"R","S":"S","W":"W","K":"M","M":"K","B":"V","V":"B","D":"H","H":"D"}

    def threeprimes(self,flank,maxstep=2):
        basen = maxstep + 3
        left,rev = self.leftright(flank,basen)
        orig = ']' + flank.split(']')[1][:basen] 
        orig = orig.upper()
        return [orig,rev]

    def fiveprimes(self,flank,maxstep=2):
        basen = maxstep + 3
        right,rev = self.rightleft(flank,basen)
        orig = flank.split('[')[0][-basen:] + '['
        orig = orig.upper()
        return [orig,rev]

def walk(qf,curs,report,maxstep=2):
    rc = qf.row_count
    fvper = int(0.20 * rc)
    logline = fvper
    start = time.time()
    count = 0
    qfit = itls(qf)
    walkf = WalkF()
    vid,pos = next(qfit)
    pos = int(pos)
    increment_set_succ = set()
    increment_set_fail = set()
    old_diff = 0
    for nxtv,nxtp in qfit:
        nxtp = int(nxtp)
        diff = nxtp - pos
        if 0 < diff <= maxstep:
            first = getflank(curs,vid)
            second = getflank(curs,nxtv)
            f3prime = walkf.threeprimes(first,maxstep)[0] #original right side (3') of first flank
            s3primes = walkf.threeprimes(second,maxstep) #array: original right side and reversed left to right side of second flank
            f5prime = walkf.fiveprimes(first,maxstep)[0] #original left side (5') of first flank
            s5primes = walkf.fiveprimes(second,maxstep) #array: original left side and reversed right to left side of second flank
            s3primes = [bs[1:4] for bs in s3primes]
            exp3psecond = f3prime[diff+1:diff+4] # +1 accounts for ']' and +4 makes the triplet 
            s5primes = [bs[-4:-1] for bs in s5primes]
            exp5psecond = f5prime[-diff-1-3:-diff-1] # -1 for '[' and -3 makes the triplet
            if exp3psecond in s3primes:
                increment_set_succ.add(vid)
                increment_set_succ.add(nxtv)
            elif exp5psecond in s5primes:
                increment_set_succ.add(vid)
                increment_set_succ.add(nxtv)
            else:
                increment_set_fail.add(vid)
                increment_set_fail.add(nxtv)
            old_diff = diff
        else: # reset sets
            if increment_set_succ:
                report.write("validated shift - %s steps - %s    " % (old_diff,','.join(increment_set_succ)))
            if increment_set_fail:
                report.write("unvalidated shift - %s steps - %s" % (old_diff,','.join(increment_set_fail)))
            if increment_set_succ or increment_set_fail:
                report.write("\n")
            increment_set_succ.clear()
            increment_set_fail.clear()
        vid = nxtv
        pos = nxtp
        count += 1
        if logline == count:
            now = int(time.time() - start)
            logging.info("approximately %.2f%% parsed (of current chromosome) after %s seconds, %s positions/lines, line: %s,%s" % ((count/rc*100),now,count,vid,pos))
            logline += fvper
    if increment_set_succ:
        report.write("validated shift - %s steps - %s    " % (old_diff,','.join(increment_set_succ)))
    if increment_set_fail:
        report.write("unvalidated shift - %s steps - %s" % (old_diff,','.join(increment_set_fail)))


def getflank(curs,vid):
    q = "SELECT flank_seq FROM flank WHERE id = %s"
    vals = (vid,)
    curs.execute(q,vals)
    flanks = curs.fetchall()
    flank = flanks[0][0] # take first one. later: may add ranking to flank seqs and then ordered output will be possible
    return flank

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
    logfile = datetime.datetime.now().strftime("walk_%a_%d%b_%I%p.log")
    logging.basicConfig(filename=logfile, level=logging.INFO)
    start = time.time()
    count = 0
    chrs = [st[0] for st in chrs if st[0] != '0']
    logging.info('chromwalk.py: found %s chromosomes to walk: %s',len(chrs),','.join(chrs))
    report = open('walk_report.txt','w')
    try:
        for chrm in chrs[:1]: #remove index[] after testing
            report.write("Chromosome %s:\n" % (chrm))
            fname = "walk_d_" + chrm + ".txt"
            q = "SELECT id,pos FROM positions WHERE chr = %s ORDER BY build,pos ASC" #LIMIT 220" # remove limit after testing
            vals = (chrm,)
            qf = QueryFile(fname,q,vals,db)
            count += qf.row_count
            logging.info('starting %s, file has %s lines',fname,qf.row_count)
            qfit = itls(qf)
            walk(qf,curs,report) 
            now = int(time.time() - start)
            logging.info('done %s, took %s seconds',fname,now)
            qf.remove()
    except Exception as e:
        statement = "error encountered during chromosome " + chrm + str(sys.exc_info()[0]) + str(e)
        logging.error(statement)
        raise 
    finally:
        report.close()
        conn.close()
    now = int(time.time() - start)
    logging.info('Finished all chromosomes after %s seconds (%s rows)' % (now,count))

if __name__ == '__main__':
    main()
