import sys
from connect import DBConnect

class QueryFile:

    def __init__(self,bfile,qry,vals=(),db="chip_comp"):
        self.bfile = bfile
        self.qry = qry
        self.vals = vals
        self.db = db
        self._makeF()   #put back for file generation

    def _makeF(self):
        try:
            f = open(self.bfile,"w")
            conn = DBConnect(self.db)
            curs = conn.getCursor()
            curs.execute(self.qry,self.vals)
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

