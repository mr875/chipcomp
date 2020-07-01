import itertools
import re 
import os
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

    def getrs(self,val):
        mo = re.search("rs[0-9]{4,}", val)         
        if mo:
            return mo.group()
        else:
            return None

    def makeit(self):
        with open(self.fname) as f:
            reader = csv.reader(f, delimiter=self.delim, quotechar=self.quotechar)
            for line in reader:
                yield line

    def fillcust(self,custom_titles):
        try:
            ordrdlst = [self.colnum(i) for i in custom_titles]
        except ValueError :
            print ("problem finding hard coded custom columns for %s:%s\ncould you be using the wrong child class (%s) for file %s?" % (self.datasource,custom_titles,self.__class__,self.datasource))
            raise
        return ordrdlst

    def comblist(self,tot):#creates a list of combinations of indexes to do sequence comparison without loosing the orginal order of the columns as they appear in the source file
        comblist = []
        for a, b in itertools.combinations(range(tot), 2):
            comblist.append([a,b])
        return comblist

    def checkflank(self,seq):
        seqsplit = []
        a = seq.split('[')
        if len(a) == 2:
            seqsplit.append(a[0])
        else:
            return None
        b = seq.split(']')
        if len(b) == 2:
            seqsplit.append(b[1])
        else:
            return None
        return seqsplit

    def flankcomp(self,seq1,seq2):
        seq1split = self.checkflank(seq1)
        seq2split = self.checkflank(seq2)
        seq1valid = False
        seq2valid = False
        if seq1split:
            seq1valid = True
        if seq2split:
            seq2valid = True
        if not seq1valid or not seq2valid:
            return seq1valid,seq2valid #leave early
        return seq1valid,seq2valid

    def choose_flankseq(self,line_arr,seq_inds):
        seqs = [line_arr[coln] for coln in seq_inds]
        comblist = self.comblist(len(seqs))
        #print(comblist)  # all comparisons
        skip = set()  
        combindxinclude = []
        for ind,comb in enumerate(comblist): 
            if set(comb) & skip:
                continue
            seq1valid, seq2valid = self.flankcomp(seqs[comb[0]],seqs[comb[1]])
            combinclude = False 
            if not seq1valid:
                skip.add(comb[0])
            else:
                combinclude = True 
            if not seq2valid:
                skip.add(comb[1])	
            else:
                combinclude = True
            if combinclude:
                combindxinclude.append(ind)	
        #print(skip,combindxinclude) #what seqs to skip and what combos to include
        seqnum = set()
        for combind in combindxinclude:
            seqnum.add(comblist[combind][0])
            seqnum.add(comblist[combind][1])
        seqnum = seqnum - skip
        #print(seqnum)  # unique and non faulty seqs to include (their indexes in self.flankseqcoln)
        return list(seqnum)

class InfCorEx24v1a1(ChipReader):

    def __init__(self,fname):
        super().__init__(fname)
        self.datasource=os.path.basename(fname)
        self.load_cols()
        self.load_custom()
        self.GRCh37='37'

    def load_cols(self):
        self.col_unique_id = self.colnum('SNP_Name') 
        self.col_chr = self.colnum('Chr')
        self.col_GRCh37_pos = self.colnum('Coord')

    def load_custom(self):
        self.flankseqcols=["Forward_Seq","Design_Seq","Top_Seq","Plus_Seq"]
        self.flankseqcoln = self.fillcust(self.flankseqcols)

    def proc_line(self,line_arr):
        seqs_to_use = self.choose_flankseq(line_arr,self.flankseqcoln)
        print(seqs_to_use)
        #todo: convert seqs_to_use back into line_arr coordinates
        #todo: remove sequence repeats from the selection
