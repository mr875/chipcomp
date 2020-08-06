from connect import DBConnect
import sys

class QueryFile:

    def __init__(self,bfile,qry,db="chip_comp"):
        self.bfile = bfile
        self.qry = qry
        self.db = db
        #self._makeF()   #put back for file generation

    def _makeF(self):
        try:
            f = open(self.bfile,"w")
            conn = DBConnect(self.db)
            curs = conn.getCursor()
            curs.execute(self.qry)
            for row in curs:
                row = [str(i) for i in row]
                f.write("\t".join(row))
                f.write("\n")
        except Exception as e:
            print("error connecting/executing query/writing to file ",sys.exc_info()[0], e)
        finally:
            f.close()
            curs.close()
            conn.close()


    def read(self):
        with open(self.bfile) as f:
            for line in f:
                yield line

def getvars(line):
    pos,tot = line.split("\t")
    print(pos,tot)

def main():

    qsamepos = "select pos,count(id) from positions where build = '37' group by pos having count(id) > 1 order by count(id) desc limit 15" # REMOVE limit after testing
#    qsamepos = ("select count(*) from positions",())
    fname = 'samepos.txt'
    qf = QueryFile(fname,qsamepos)
    for line in qf.read():
        getvars(line)

if __name__ == '__main__':
    main()
#variant = VariantI(curs,dic,new_ds,build)
