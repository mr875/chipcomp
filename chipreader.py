import csv

class ZeroLimit(Exception):
    pass

class ChipReader:

    def __init__(self,fname,delim=',',quote='"'):
        self.fname = fname
        self.delim = delim 
        self.quotechar = quote
        self.it = self.makeit()
        self.header = next(self.it)
        self.datasource = None
        self.col_unique_id = None
        self.col_dbSNP_id = None
        self.col_chr = None
        self.col_GRCh37_pos = None
        self.col_GRCh38_pos = None
        self.col_flank_seq = None
        self.col_flank_strand = None
        self.col_probe_seq = None
        self.col_probe_strand = None


    def linebyline(self,limit=None):
        count=0
        if limit is not None and limit < 1:
            raise ZeroLimit("function needs a non zero limit argument")
        for l in self.it:
            if not limit:
                yield l
            elif count == (limit - 1):
                yield l
                return
            else:
                count+=1
                yield l

    def colnum(self,title):
        ind = self.header.index(title)
        return ind

    def makeit(self):
        with open(self.fname) as f:
            reader = csv.reader(f, delimiter=self.delim, quotechar=self.quotechar)
#            return reader
            for line in reader:
                yield line

class InfCorEx24v1a1(ChipReader):

    def __init__(self,fname):
        super().__init__(fname)

    def load_cols():
        self.col_unique_id = self.colnum('SNP_Name') 
