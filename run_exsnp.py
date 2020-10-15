from varianti import VariantI
from queryfile import NormFile
from connect import DBConnect
import sys

'''
chipcursor.execute(query2)
for row in chipcursor.fetchall():
    print(row)
'''
class VariantRS(VariantI):
    
    def __init__(self,curs,nonrs,rs,ds='dbsnp',build='38'):
        self.curs = curs
        self.secondary_id = nonrs
        self.dbsnpid = rs
        self.datasource = ds

    def log_dbsnpid(self):
        rs_ds = self.id_exists(self.dbsnpid)
        if rs_ds:
            print("rsid %s already in consensus table, from datasource %s" % (self.dbsnpid,rs_ds))
            return
        nonrs_ds = self.id_exists(self.secondary_id)
        if not nonrs_ds:
            print ("expecting to find non rsid '%s' in consensus table but did not." % (self.secondary_id))
            return
        nonrs_ds = nonrs_ds[0]
        #print("rsid %s to be added to consensus table, datasource '%s', in place of %s (ds %s)" % (self.dbsnpid,self.datasource,self.secondary_id,nonrs_ds))
        self.snpid_swapin(self.secondary_id,self.dbsnpid,nonrs_ds,self.datasource)

def main(argv):
    fname = 'exsnp/nors_j_dbsnp.txt'
    if argv:
        fname = argv[0]
    else:
        print("no file provided, instead using default file %s to act as 3 column join file (chr:pos old_id rsid)" % (fname))
    joinfile = NormFile(fname)
    linecount = joinfile.row_count
    oneper = int(linecount/100)
    add = oneper
    db = DBConnect("chip_comp")
    curs = db.getCursor()
    for num,cols in enumerate(joinfile.readls(" ")):
        try:
            var_rs = VariantRS(curs,cols[1],cols[2])
            var_rs.log_dbsnpid()
            db.commit()
            if num == oneper:
                print('%s percent done' % ((oneper/linecount)*100))
                oneper += add
        except:
            print("Unexpected error with %s: %s" % (cols,sys.exc_info()[0]))
            db.close()
            raise
#        if num == 1:
#            break
    db.close()

if __name__ == '__main__':
    main(sys.argv[1:])

