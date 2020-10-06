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
    valind = None
    for ind,dbfl in enumerate(dbflanks):
        match = VariantI.flankmatch(nflnk,dbfl)
        if match:
            valind = ind
            break
    return valind

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
    for uid,nflnk in listf.readls():
        allfl = get_flank(uid,curs)
        match = compare_dbf(nflnk,allfl)
        if match is None:
            revnflnk = rev(nflnk)
            match = compare_dbf(revnflnk,allfl)
        if match is not None:
            print('found match:',nflnk,allfl[match]['flank_seq'])
        else:
            print('no match for',nflnk)
    conn.close()

if __name__ == '__main__':
    main(sys.argv[1:])

