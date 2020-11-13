import sys
from queryfile import NormFile
from connect import DBConnect

def buildrsmerge(curs,rsid,refpos,altid):
    q = 'SELECT build,concat(chr,":",pos) FROM positions WHERE id = %s'
    vals = (rsid,)
    curs.execute(q,vals)
    tsev = 0
    teight = 0
    dbrefpos38 = []
    dbrefpos37 = []
    for build,chrpos in curs.fetchall():
        if build == '37':
            dbrefpos37.append(chrpos)
            tsev +=1
        elif build == '38':
            dbrefpos38.append(chrpos)
            teight += 1
    if teight < 1 and tsev > 0:
        print("rsid %s on build 37 can be merged with %s (build 38)" % (rsid,altid)) # but is altid present in build 37 as well? if so it will have different coords or it will have been merged at a previous stage
        q = q + ' AND build = %s'
        curs.execute(q,(altid,'37'))
        b37s = curs.fetchall() 
        print(len(b37s))
    if teight > 0 and '0:0' not in dbrefpos38:
        print("rsid %s exists in db with build 38 coords, can not merge with suggested %s" % (rsid,altid)) 
    if teight > 0 and '0:0' in dbrefpos38:
        print("rsid %s already exists under build 38 but with no coords (0:0)" % (rsid))

def xyrsmerge(curs,rsid,refpos,altid):
    print("entered xyrsmerge")

def main(argv):
    
    if len(argv) < 1:
        print("provide file exported from exsnp.py output, with possible build 37 merges and XY/X/Y possible rs id alternatives. Format: rsid chr:coord existing_alt_id. Sometimes called j_already.txt")
        return
    else:
        infile = NormFile(argv[0])
    conn = DBConnect("cc4")
    curs = conn.getCursor()
    for rsid,refpos,altid in infile.readls(dlim=" "):
        if refpos.split(':')[0].isnumeric():
            try:
                buildrsmerge(curs,rsid,refpos,altid)
            except:
                conn.close()
                raise
        else:
            xyrsmerge(curs,rsid,refpos,altid)
    conn.close()

if __name__ == '__main__':
    main(sys.argv[1:])

