import sys
from queryfile import NormFile
from connect import DBConnect
from varianti import VariantM

def buildrsmerge(curs,rsid,refpos,altid):
    q = 'SELECT build,concat(chr,":",pos),datasource FROM positions WHERE id = %s'
    vals = (rsid,)
    curs.execute(q,vals)
    tsev = 0
    teight = 0
    dbrefpos38 = []
    dbrefpos37 = []
    for build,chrpos,ds in curs.fetchall():
        if build == '37':
            dbrefpos37.append(chrpos)
            tsev +=1
        elif build == '38':
            dbrefpos38.append(chrpos)
            teight += 1
    if teight < 1 and tsev > 0:
        q = q + ' AND build = %s'
        curs.execute(q,(altid,'37'))
        b37s = curs.fetchall() 
        if len(b37s) > 0:
            print('rsid %s can not be merged with alt_id %s because alt_id has a build 37 position (%s) (would have been merged at earlier stage if they matched)' % (rsid,altid,len(b37s)))
            return
        print("attempting merging of rsid %s on build 37 with %s on build 38" % (rsid,altid)) 
        curs.execute(q,(altid,'38'))
        b38s = curs.fetchall()
        # first insert rsid into build 38 so it can be merged with alt_id (that's in build 38)
        ins = 'INSERT IGNORE INTO positions (id,chr,pos,build,datasource) VALUES (%s,%s,%s,%s,%s)'
        curs.execute(ins,(rsid,refpos.split(':')[0],refpos.split(':')[1],'38','dbsnp'))
        VariantM(curs,rsid,refpos.split(':')[1],'38','dbsnp',refpos.split(':')[0]).snpid_swapin(altid,b38s[0][2])
    if teight > 0 and '0:0' not in dbrefpos38:
        print("rsid %s exists in db with build 38 coords, can not merge with suggested %s" % (rsid,altid)) 
    if teight > 0 and '0:0' in dbrefpos38:
        print("rsid %s already exists under build 38 but with no coords (0:0)" % (rsid))


def xyrsmerge(curs,rsid,refpos,altid):
    q = 'SELECT chr FROM positions WHERE id = %s'
    val = (rsid,)
    curs.execute(q,val)
    chrs = curs.fetchall()
    chrs = set([c[0] for c in chrs])
    chrs.add(refpos.split(':')[0])
    if len(chrs) == 1:
        buildrsmerge(curs,rsid,refpos,altid)
        return
    print('adding %s as alt_id to %s because rsid exists already but with different X/Y/XY chr' % (rsid,altid))
    VariantM(curs,None,None,None,None,None).add_altid(altid,rsid,'dbsnp')

def main(argv):
    
    if len(argv) < 1:
        print("provide file exported from exsnp.py output, with possible build 37 merges and XY/X/Y possible rs id alternatives. Format: rsid chr:coord existing_alt_id. Sometimes called j_already.txt")
        return
    else:
        infile = NormFile(argv[0])
    conn = DBConnect("cc4")
    curs = conn.getCursor()
    for rsid,refpos,altid in infile.readls(dlim=" "):
        try:
            if refpos.split(':')[0].isnumeric():
                buildrsmerge(curs,rsid,refpos,altid)
            else:
                xyrsmerge(curs,rsid,refpos,altid)
            conn.commit()
        except:
            conn.close()
            raise
    conn.close()

if __name__ == '__main__':
    main(sys.argv[1:])

