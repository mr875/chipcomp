from varianti import VariantI
from connect import DBConnect
from queryfile import NormFile
import sys

def get_flank(uid,curs):
    q = "SELECT * FROM flank WHERE id = %s"
    curs.execute(q,(uid,))
    allfl = curs.fetchall()
    return allfl

def compare_dbf(nflnk,allfl):
    dbflanks = [f['flank_seq'] for f in allfl]
    valind = []
    for ind,dbfl in enumerate(dbflanks):
        match = VariantI.flankmatch(nflnk,dbfl)
        if match:
            valind.append(ind)
    return valind

def rev(seq): # repeated: should reuse already-written methods but original design does not facilitate this well enough 
    switchdic = {"A":"T","C":"G","G":"C","T":"A","[":"]","]":"["}
    #switchextra = {"N":"N","R":"Y","Y":"R","S":"S","W":"W","K":"M","M":"K","B":"V","V":"B","D":"H","H":"D"}
    #switchdic = {**self.switchdic,**self.switchextra}
    revseq = ''.join(switchdic[n] for n in seq[::-1])
    return revseq
    
def main(argv):
    longerf = open("exfl_longer.txt", "w")
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
    count_needlonger = 0
    count_matchfound = 0
    count_matchnotfound = 0
    for uid,nflnk in listf.readls():
        allfl = get_flank(uid,curs)
        match = compare_dbf(nflnk,allfl)
        revnflnk = rev(nflnk)
        match += compare_dbf(revnflnk,allfl)
        if match:
            dbfl = [allfl[ind]['flank_seq'] for ind in match]
            #print('found %s match(es): %s :::: %s' % (len(dbfl),nflnk,' AND '.join(dbfl)))
            if len(match) > 1:
                count_needlonger += 1
                fvplens = [len(df.split('[')[0]) for df in dbfl]
                print(fvplens)
                longerf.write('%s\t%s\n' % (uid,max(fvplens)))
            else:
                count_matchfound += 1
        else:
            count_matchnotfound += 1
            print('no match for',uid,nflnk)
    print('%s variants matched 1 of their flank sequences\n%s variants need longer external flanks to find a match\n%s variants do not have any db flanks matching the external flank sequence' % (count_matchfound,count_needlonger,count_matchnotfound))
    conn.close()
    longerf.close()

if __name__ == '__main__':
    main(sys.argv[1:])

