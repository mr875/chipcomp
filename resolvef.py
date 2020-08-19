from chipreader import ChipReader
from connect import DBConnect
import time
import datetime
import logging
from queryfile import QueryFile

class ResolveF(ChipReader):

    def __init__(self,allfl):
        self.allfl = allfl
        self.combos = self.comblist(len(self.allfl))
        self.choose_flankseq()

    def choose_flankseq(self):
        remove = set()  
        keep = []
        for ind,comb in enumerate(self.combos): 
            if set(comb) & remove:
                continue
            seq1 = self.allfl[comb[0]]['flank_seq']
            seq2 = self.allfl[comb[1]]['flank_seq']
            seq1keep, seq2keep = self.flankcomp(seq1,seq2)  
            if seq1keep and seq2keep:
                seq1keep, seq2keep = self.flankcomp(seq1,seq2,rev=True)
            print("%s:%s\n%s:%s" % (str(seq1keep),seq1,str(seq2keep),seq2))
            print()

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
    conn = DBConnect(db)
    curs = conn.getCursor(dic=True)
    for line in qf.read():
        print(line)
        allfl = getvars(line,curs)
        fr = ResolveF(allfl)
    conn.close()


if __name__ == '__main__':
    main()
