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
        self.local_alts = False
        self.switchdic = {"A":"T","C":"G","G":"C","T":"A"}
        self.switchextra = {"N":"N","R":"Y","Y":"R","S":"S","W":"W","K":"M","M":"K","B":"V","V":"B","D":"H","H":"D"}
        if self.check_local(): # bp shift detection
            self.local = True
            self.remove = self.choose_flankseq()
        else:
            for fl in self.allfl: #bp shift or just an 'N' in one of the near-snp locations that doesn't match?
                left,right = self.leftright(fl['flank_seq'])
                if self.contains_alt(left) or self.contains_alt(right):
                    self.local_alts = True

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

    def remove_red(self,curs): 
        q = 'DELETE FROM flank WHERE id = %s AND colname = %s AND datasource = %s' # these 3 cols = primary key
        for ind in self.remove:
            fl = self.allfl[ind]
            args = (fl['id'],fl['colname'],fl['datasource'])
            curs.execute(q,args)

    def check_local(self):
        model = self.allfl[0]['flank_seq']
        left,right = self.leftright(model)
        for fl in self.allfl[1:]:
            comp = fl['flank_seq'].upper()
            if left not in comp and right not in comp:
                return False
        return True
    
    def contains_alt(self,seq):
        seq = seq.upper()
        for key in self.switchextra:
            if seq.find(key) > -1: #find() returns -1 if character not found, otherwise returns index
                return True
        return False

    def leftright(self,flank,bps=5): #grab left bit before [variant] and reverse compliment it: LLL[ -> ]RRR
        flank = flank.upper()
        swaps = {**self.switchdic,**self.switchextra,"[":"]"}       
        left = flank.split('[')[0] + '['
        leftend = left[-(bps+1):]
        rightbeg = ''.join(swaps[n] for n in leftend[::-1])
        return leftend,rightbeg

    def rightleft(self,flank,bps=5): #grab right but after [variant] and reverse compliment it: ]RRR -> LLL[
        flank = flank.upper()
        swaps = {**self.switchdic,**self.switchextra,"]":"["} 
        right = ']' + flank.split(']')[1]
        rightbeg = right[:bps+1]
        leftend = ''.join(swaps[n] for n in rightbeg[::-1])
        return rightbeg,leftend

    def rev(self,seq): # https://github.com/cathalgarvey/dna2way/blob/master/dnahash.py
        switchdic = {**self.switchdic,**self.switchextra}
        revseq = ''.join(switchdic[n] for n in seq[::-1])
        return revseq

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

    db = 'chip_comp'
    q = "SELECT id,COUNT(flank_seq) FROM flank GROUP BY id HAVING COUNT(flank_seq) > %s ORDER BY COUNT(flank_seq) DESC"
    #q = "SELECT id,chr FROM consensus where id = %s"
    vals = (1,)
    #vals = ('rs150542670',)
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
    local_alts = 0 # counts when an alternative base (like 'N') appears within 5 bases of the variant IF it is unique compared with other flanks under th same id (for logging purposes because it prevents bp shift detection for the id)
    conn = DBConnect(db)
    curs = conn.getCursor(dic=True)
    for line in qf.read():
        try:
            allfl = getvars(line,curs)
            fr = ResolveF(allfl)
            if fr.local:
#                fr.print_result()
                fr.remove_red(curs)
            else:
                if fr.local_alts:
                    local_alts += 1
                else:
                    logging.warning("line [%s] not filtered because local variant location appears to not be consistent" % (line.rstrip(),))
            count += 1
            if logline == count:
                now = int(time.time() - start)
                logging.info("approximately %.2f%% parsed after %s seconds, %s positions, line: %s" % ((count/rc*100),now,count,line.rstrip()))
                logline += fvper
        except KeyError as ke:
            dil_ds = False
            for fl in allfl:
                if fl['datasource'] == '114': # flank seqs from datasource 'DIL-taqman' are not in a format that can be compared (yet) so skip this variant
                    dil_ds = True
            if not dil_ds:
                logging.error("unexpected KeyError, re-raising error for variant/line: %s" % (line.rstrip()))
                conn.rollback()
                conn.close()
                raise
            logging.info("detected key KeyError but this id has a DIL-taqman datasource and these flanks are not comparable yet so skipping " + line.rstrip())
        except Exception as e:
            conn.rollback()
            conn.close()
            statement = "error at merging step for line " + line + str(sys.exc_info()[0]) + str(e)
            logging.error(statement)
            raise 
        else:
            conn.commit()
    now = int(time.time() - start)
    logging.info('Finished after %s seconds (%s rows)' % (now,count))
    logging.info('%s ids have alternative bases (like N) in close proximity to the variant position (uniquely) in some flank sequences. Basic base shift detection not possible in these cases' % (local_alts))
    conn.close()


if __name__ == '__main__':
    main()
