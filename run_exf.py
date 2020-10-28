from resolvef import ResolveF
from varianti import VariantI
from connect import DBConnect
from queryfile import NormFile
import re
import sys
import copy

class ResExf(ResolveF):

    def __init__(self,allfl):
        self.allfl = allfl
        self.switchdic = {"A":"T","C":"G","G":"C","T":"A"}
        self.switchextra = {"N":"N","R":"Y","Y":"R","S":"S","W":"W","K":"M","M":"K","B":"V","V":"B","D":"H","H":"D"}
        self.knownstrandflags = ["+","PLUS","TOP"]
        self.knowncolnameflags = ["Forward_Seq","Plus_Seq","TopGenomicSeq","Plus_Seq"]
        self.combos = self.comblist(len(self.allfl))

    def check_local(self,modelseq):
        model = modelseq
        valind = []
        left,right = self.leftright(model)
        for ind,fl in enumerate(self.allfl):
            comp = fl['flank_seq'].upper()
            if left in comp or right in comp:
                valind.append(ind)
        return valind
    
    @classmethod
    def remove_red(cls,curs,fordels): 
        q = 'DELETE FROM flank WHERE id = %s AND colname = %s AND datasource = %s' # these 3 cols = primary key
        for fl in fordels:
            args = (fl['id'],fl['colname'],fl['datasource'])
            curs.execute(q,args)

    @classmethod
    def flag_chosen(cls,curs,chfls):
        q = 'UPDATE flank SET chosen = 1 WHERE id = %s AND colname = %s AND datasource = %s'
        for fl in chfls:
            args = (fl['id'],fl['colname'],fl['datasource'])
            curs.execute(q,args)
            
def alr_chose(matchfl):
    already_chosen = [fld['chosen'] for fld in matchfl]
    if 1 in already_chosen or 2 in already_chosen:
        print("this list of flanks already has a 'chosen' row ", matchfl)
        return True
    return False

def multchoose(matchfl):
    already_chosen = [fld['chosen'] for fld in matchfl]
    if alr_chose(matchfl):
        return [],[]
    remove = ResExf(matchfl).choose_flankseq()
    if (len(remove)+1) < len(matchfl):
        indc_fl = copy.deepcopy(matchfl)
        for fld in indc_fl:
            fld['flank_seq'] = indel_correction(fld['flank_seq'])
        remove = ResExf(indc_fl).choose_flankseq()
    if (len(remove)+1) < len(matchfl):
        print("not found all removals, probably due to indels. Will choose the first and remove the rest ",matchfl)
        keep = {0}
        remove = {i for i in range(len(matchfl))[1:]}
    keep = {i for i in range(len(matchfl))}.difference(remove)
    return list(remove),list(keep)

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
    editdb = False
    if len(argv) > 0:
        exfname = argv[0]
    try:
        listf = NormFile(exfname)
    except FileNotFoundError:
        print("file %s does not exist. Provide an external flank file (<id>\\t<flankseq>)" % (exfname))
        raise
    if len(argv) > 1:
        if argv[1] == 'True':
            editdb = True
    print(editdb)
    conn = DBConnect(db)
    curs = conn.getCursor(dic=True)
    badmatchf = open("exfl_badmatch","w")
    longerf = open("exfl_longer.txt", "w")
    if editdb: 
        longerf.write('no entries because "editdb" switch is on\n')
    count_matchmult = 0
    count_allmatch = 0
    count_matchone = 0
    count_matchzero = 0
    for uid,nflnk in listf.readls():
        try:
            allfl = get_flank(uid,curs)
        except:
            print("Unexpected error:", sys.exc_info()[0],'\ninterrupted at uid ',uid)
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
                if not editdb:
                    fvplens = [len(df['flank_seq'].split('[')[0]) for df in matchfl]
                    longerf.write('%s\t%s\n' % (uid,max(fvplens)))
                else:
                    remove,keep = multchoose(matchfl)
                    fordel = [matchfl[ind] for ind in remove]
                    forkeep = [matchfl[ind] for ind in keep] # expecting 1 entry
                    try:
                        pass
                    #    ResExf.remove_red(curs,fordel)
                    #    ResExf.flag_chosen(curs,forkeep)
                    #    conn.commit()
                    except:
                        print("Unexpected error while editing db:", sys.exc_info()[0],'\ninterrupted at uid ',uid)
                        break
            else:
                count_matchone += 1
                if not alr_chose(matchfl):
                    try:
                        ResExf.flag_chosen(curs,matchfl)
                        conn.commit()
                    except:
                        print("Unexpected error while editing db:", sys.exc_info()[0],'\ninterrupted at uid ',uid)
                        break
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

