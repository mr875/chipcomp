from chipreader import ChipReader
import sys
from connect import DBConnect
import time
import datetime
import logging
from queryfile import QueryFile

class ResolveF(ChipReader):


    def __init__(self,allfl):
        self.knownstrandflags = ["+","PLUS","TOP"]
        self.knowncolnameflags = ["Forward_Seq","Plus_Seq","TopGenomicSeq","Plus_Seq"]
        self.allfl = allfl
        self.combos = self.comblist(len(self.allfl))
        self.local = False
        if self.check_local():
            self.local = True
            self.remove = self.choose_flankseq()

    def print_result(self): # for testing
        print("REMOVE:")
        for ind in self.remove:
            fl = self.allfl[ind]
            print(fl['colname'],fl['flank_strand'],fl['flank_seq']) 
        print("KEEP:")
        for ind,fl in enumerate(self.allfl):
            if ind in self.remove:
                continue
            print(fl['colname'],fl['flank_strand'],fl['flank_seq']) 

    def check_local(self):
        model = self.allfl[0]['flank_seq']
#        print(model)
        left,right = self.leftright(model)
#        print(left,right)
        for fl in self.allfl[1:]:
            comp = fl['flank_seq'].upper()
            if left not in comp and right not in comp:
                return False
        return True

    def choose_flankseq(self):
        remove = set()  
        combindxinclude = []
        for ind,comb in enumerate(self.combos): 
            if set(comb) & remove:
                continue
            seq1 = self.allfl[comb[0]]['flank_seq']
            seq2 = self.allfl[comb[1]]['flank_seq']
            seq1keep, seq2keep = self.flankcomp(seq1,seq2)  
            if seq1keep and seq2keep:
                seq1keep, seq2keep = self.flankcomp(seq1,seq2,rev=True)
            #print("%s:%s\n%s:%s" % (str(seq1keep),seq1,str(seq2keep),seq2))
            #print()
            if seq1keep and seq2keep:
                continue
            # one will be false from now, add check?
            goodcol1 = self.goodcol(comb[0])
            goodcol2 = self.goodcol(comb[1])
            if seq1:
               rem = comb[1]
               if not goodcol1:
                   if goodcol2:
                       rem = comb[0]
            else:
                rem = comb[0]
                if not goodcol2:
                    if goodcol1:
                        rem = comb[1]
            remove.add(rem)
        return remove

    def goodcol(self,ind):
        flk_str = self.allfl[ind]['flank_strand']
        colname = self.allfl[ind]['colname']
        if flk_str in self.knownstrandflags:
            return True
        if colname in self.knowncolnameflags:
            return True
        return False
            

def getvars(line,curs):
    idf = line.split("\t")[0]
    q = 'SELECT * FROM flank WHERE id = %s'
    curs.execute(q,(idf,))
    allfl = curs.fetchall()
    return allfl

def main():

    db = 'cc2'
    q = "SELECT id,COUNT(flank_seq) FROM flank GROUP BY id HAVING COUNT(flank_seq) > %s ORDER BY COUNT(flank_seq) DESC limit 10" # REMOVE limit after testing
    vals = (1,)
    fname = 'twos.txt'
    qf = QueryFile(fname,q,vals,db)
    rc = qf.row_count
    fvper = int(0.01 * rc)
    logline = fvper
    logfile = datetime.datetime.now().strftime("resflk_%a_%d%b_%I%p.log")
    logging.basicConfig(filename=logfile, level=logging.INFO)
    logging.info('resolvef.py: created %s with %s rows, using db %s',fname,rc,db)
    start = time.time()
    count = 0
    conn = DBConnect(db)
    curs = conn.getCursor(dic=True)
    for line in qf.read():
        print(line)
        try:
            allfl = getvars(line,curs)
            fr = ResolveF(allfl)
            if fr.local:
                fr.print_result()
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
    now = int(time.time() - start)
    logging.info('Finished after %s seconds (%s rows)' % (now,count))
    conn.close()


if __name__ == '__main__':
    main()
