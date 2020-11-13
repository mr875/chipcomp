from varianti import VariantI
from queryfile import NormFile
from connect import DBConnect
import sys

class VariantRS(VariantI):
    
    second_rs = {}
    
    def __init__(self,curs,nonrs,rs,ds='dbsnp',build='38'):
        self.curs = curs
        self.secondary_id = nonrs
        self.dbsnpid = rs
        self.datasource = ds

    def log_dbsnpid(self,second=0):
        rs_ds = self.id_exists(self.dbsnpid)
        if rs_ds:
            print("rsid %s already in consensus table, from datasource %s" % (self.dbsnpid,rs_ds))
            return
        nonrs_ds = self.id_exists(self.secondary_id)
        if not nonrs_ds:
            if second:
                if self.secondary_id in VariantRS.second_rs:
                    #print("found second rs. %s added to existing %s as alt id for original id %s" % (self.dbsnpid,VariantRS.second_rs[self.secondary_id],self.secondary_id))
                    self.add_altid(VariantRS.second_rs[self.secondary_id],self.dbsnpid,self.datasource)
                else:
                    print("original id %s not found so expected to find in VariantRS.second_rs dict but did not. %s not added to alt_ids" % (self.secondary_id,self.dbsnpid))
            else:
                print ("expecting to find non rsid '%s' in consensus table but did not." % (self.secondary_id))
            return
        nonrs_ds = nonrs_ds[0]
        #print("rsid %s to be added to consensus table, datasource '%s', in place of %s (ds %s)" % (self.dbsnpid,self.datasource,self.secondary_id,nonrs_ds))
        if second:
            VariantRS.second_rs[self.secondary_id] = self.dbsnpid
        self.snpid_swapin(self.secondary_id,self.dbsnpid,nonrs_ds,self.datasource)

def main(argv):
    fname = 'exsnp/nors_j_dbsnp.txt'
    dup = 0
    if len(argv) > 0:
        fname = argv[0]
    else:
        print("no file provided, instead using default file %s to act as 3 column join file (chr:pos old_id rsid)" % (fname))
    if len(argv) > 1:
        dup = int(argv[1])
    joinfile = NormFile(fname)
    linecount = joinfile.row_count
    oneper = int(linecount/100)
    add = oneper
    db = DBConnect("cc4")
    curs = db.getCursor()
    for num,cols in enumerate(joinfile.readls(" ")):
        try:
            var_rs = VariantRS(curs,cols[1],cols[2])
            var_rs.log_dbsnpid(dup)
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

