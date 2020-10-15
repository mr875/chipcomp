import sys
import os
from connect import DBConnect

class QueryFile:

    def __init__(self,bfile,qry,vals=(),db="chip_comp"):
        self.bfile = bfile
        self.qry = qry
        self.vals = vals
        self.db = db
        self.row_count = 0
        self._makeF()   #put back for file generation

    def _makeF(self):
        try:
            f = open(self.bfile,"w")
            conn = DBConnect(self.db)
            curs = conn.getCursor()
            curs.execute(self.qry,self.vals)
            for row in curs:
                self.row_count += 1
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

    def readls(self,dlim="\t"):
        with open(self.bfile) as f:
            for line in f:
                line = line.rstrip().split(dlim)
                yield line

    def remove(self):
        os.remove(self.bfile)

class NormFile(QueryFile):

    def __init__(self,bfile):
        self.bfile = bfile
        self.row_count = self.wcl(bfile)

    def wcl(self,fname):
        with open(fname) as f:
            line_count = 0
            for line in f:
                line_count += 1
            return line_count

    def _makeF(self):
        print("_makeF not available from NormFile instance")
        print("use parent class QueryFile")
