from resolvef import ResolveF
from varianti import VariantI
from connect import DBConnect
from queryfile import NormFile
import re
import sys

class ResExf(ResolveF):

    def __init__(self,allfl):
        self.allfl = allfl
        self.switchdic = {"A":"T","C":"G","G":"C","T":"A"}
        self.switchextra = {"N":"N","R":"Y","Y":"R","S":"S","W":"W","K":"M","M":"K","B":"V","V":"B","D":"H","H":"D"}

    def check_local(self,modelseq):
        model = modelseq
        valind = []
        left,right = self.leftright(model)
        for ind,fl in enumerate(self.allfl):
            comp = fl['flank_seq'].upper()
            if left in comp or right in comp:
                valind.append(ind)
        return valind
    
def get_flank(uid,curs):
    q = "SELECT * FROM flank WHERE id = %s"
    curs.execute(q,(uid,))
    allfl = curs.fetchall()
    return allfl

def indel_correction(flank):
    if "[-/" in flank: #insertions of more than 1 bp change the sequence on the right hand side of ']' so shift all but the 1st one: GCG[-/CA]CAGA becomes GCG[-/C]ACAGA. This is just for matching purposes. the edit is not maintained anywhere
        dpart = re.findall(r"\[-/([A-Za-z]+)",flank)[0]
        shiftpart = dpart[1:]
        keeppart = dpart[0]
        left = flank.split('[')[0]
        right = flank.split(']')[1]
        newflank = left + '[-/' + keeppart + ']' + shiftpart + right
        flank = newflank
    return flank

def log_badmatch(fh,uid,nseq,flds):
    for fld in flds:
        ds = fld['datasource']
        cname = fld['colname']
        badseq = fld['flank_seq']
        fh.write('%s\t%s\t%s\t%s\t%s\n' % (uid,ds,cname,badseq,nseq))

def compare_dbf(nflnk,allfl):
    dbflanks = [f['flank_seq'] for f in allfl]
    valind = []
    for ind,dbfl in enumerate(dbflanks):
        match = VariantI.flankmatch(nflnk,dbfl)
        if match:
            valind.append(ind)
        else:
            match = VariantI.flankmatch(nflnk,indel_correction(dbfl)) # try again
            if match:
                valind.append(ind)
    return valind

def findnomatch(reducedDL,originalDL):#which elements in original dictionary list are not in the reduced dictionary list 
    rdprimkeys = [rd['id'] + rd['colname'] + rd['datasource'] for rd in reducedDL]
    odprimkeys = [od['id'] + od['colname'] + od['datasource'] for od in originalDL]
    nomatch = []
    for ind,pk in enumerate(odprimkeys):
        if pk not in rdprimkeys:
            nomatch.append(ind)
    nomatchfl = [originalDL[ind] for ind in nomatch]
    return nomatchfl

def rev(seq): # repeated: should reuse already-written methods but original design does not facilitate this well enough 
    switchdic = {"A":"T","C":"G","G":"C","T":"A","[":"]","]":"["}
    #switchextra = {"N":"N","R":"Y","Y":"R","S":"S","W":"W","K":"M","M":"K","B":"V","V":"B","D":"H","H":"D"}
    #switchdic = {**self.switchdic,**self.switchextra}
    revseq = ''.join(switchdic[n] for n in seq[::-1])
    return revseq
    
def main(argv):
    exfname = 'exflank/external_flanks.txt' # add alternative path on command line
    db = 'chip_comp'
    if argv:
        exfname = argv[0]
    try:
        listf = NormFile(exfname)
    except FileNotFoundError:
        print("file %s does not exist. Provide an external flank file (<id>\\t<flankseq>)" % (exfname))
        raise
    conn = DBConnect(db)
    curs = conn.getCursor(dic=True)
    badmatchf = open("exfl_badmatch","w")
    longerf = open("exfl_longer.txt", "w")
    count_matchmult = 0
    count_allmatch = 0
    count_matchone = 0
    count_matchzero = 0
    for uid,nflnk in listf.readls():
        try:
            allfl = get_flank(uid,curs)
        except:
            print("Unexpected error:", sys.exc_info()[0])
            break
        match = compare_dbf(nflnk,allfl)
        revnflnk = rev(nflnk)
        match += compare_dbf(revnflnk,allfl)
        if match:
            matchfl =  [allfl[ind] for ind in match] # new list with matching dicts (so new indexes)
            localmatch = ResExf(matchfl).check_local(nflnk) # additional check, potentially reducing matches
            matchfl = [matchfl[ind] for ind in localmatch]
            nomatchfl = findnomatch(matchfl,allfl)
            if len(localmatch) > 1:
                count_matchmult += 1
                if len(localmatch) == len(allfl):
                    count_allmatch += 1
                fvplens = [len(df['flank_seq'].split('[')[0]) for df in matchfl]
                longerf.write('%s\t%s\n' % (uid,max(fvplens)))
            else:
                count_matchone += 1
            log_badmatch(badmatchf,uid,nflnk,nomatchfl)
        else:
            count_matchzero += 1
            log_badmatch(badmatchf,uid,nflnk,allfl)
            #print('no match for',uid,nflnk)
    print('%s variants matched 1 of their flank sequences\n%s variants matched multiple of their external flanks (%s of these match ALL of their flanks(so no mismatches at all))\n%s variants do not have any db flanks matching the external flank sequence' % (count_matchone,count_matchmult,count_allmatch,count_matchzero))
    conn.close()
    longerf.close()
    badmatchf.close()

if __name__ == '__main__':
    main(sys.argv[1:])

