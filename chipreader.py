import itertools
import re 
import os
import csv

class ZeroLimit(Exception):
    pass

class StrandError(Exception):
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
        self.tit_flank_seq = None
        self.col_flank_seq = None #if multiple use flankseqcols, self.colnum('self.tit_flank_seq')
        self.col_flank_strand = None # if multiple use flankseqcols_strand
        self.flankseqcols = None # col titles of flank seqs when there are multiple
        self.flankseqcoln = None # col number ^ fillcust(self.flankseqcols)
        self.flankseqcols_strand = None # col titles of flank strand, corresponding to flankseqcols 
        self.flankseqcoln_strand = None # col number of flank strand, filled with ^
        self.col_probe_seq = None # if multiple use probseq_cols
        self.col_probe_strand = None #if multiple use probstrand_cols 
        self.probseq_cols = None # col titles of probes
        self.probseq_coln = None #col number of probe seqs (fillcust(self.probseq_cols))
        self.probstrand_cols = None # col titles of probe strand
        self.probstrand_coln = None # col number of prob strand (fillcust(self.probstrand_cols))
        self.datasource=os.path.basename(fname)
        self.load_cols()
        self.load_custom()

    def load_cols(self):
        raise NotImplementedError

    def load_custom(self):
        raise NotImplementedError

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
        #seq1valid=seq2valid=False #reset
        seq1expunge = (seq1split[0]+seq1split[1]).upper()
        seq2expunge = (seq2split[0]+seq2split[1]).upper()
        return self.gencomp(seq1expunge,seq2expunge)

    def gencomp(self,seq1,seq2):
        seq1valid = False
        seq2valid = False
        if len(seq1) > 25:
            seq1valid = True
        if len(seq2) > 25:
            seq2valid = True
        if not seq1valid or not seq2valid:
            return seq1valid,seq2valid  #leave early
        seq1valid=seq2valid=False
        if len(seq1) >= len(seq2):
            seq1valid = True
            if seq2 not in seq1:
                seq2valid = True
        else:
            seq2valid = True
            if seq1 not in seq2:
                seq1valid = True
        return seq1valid,seq2valid

    def choose_flankseq(self,line_arr,seq_inds,flank = True):
        seqs = [line_arr[coln] for coln in seq_inds]
        comblist = self.comblist(len(seqs))
        #print(comblist)  # all comparisons
        skip = set()  
        combindxinclude = []
        for ind,comb in enumerate(comblist): 
            if set(comb) & skip:
                continue
            if flank:
                seq1valid, seq2valid = self.flankcomp(seqs[comb[0]],seqs[comb[1]])
            else:
                seq1valid, seq2valid = self.gencomp(seqs[comb[0]],seqs[comb[1]])
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

    def fill_flankseqs(self,line_arr,line_dict,single=False):
        if single:
            line_dict['flankseq'] = line_arr[self.col_flank_seq]
            line_dict['flankseq_colname'] = self.tit_flank_seq
            if self.col_flank_strand:
                line_dict['flankstrand_val'] = line_arr[self.col_flank_strand]
            return
        seqs_to_use = self.choose_flankseq(line_arr,self.flankseqcoln)
        cols_used = [self.flankseqcols[i] for i in seqs_to_use]
        coln_used = [self.flankseqcoln[i] for i in seqs_to_use]
        seqs = [line_arr[i] for i in coln_used]
        # print(seqs_to_use) print(cols_used) print(coln_used) print(seqs)
        line_dict['flankseq_colnames'] = cols_used
        line_dict['flankseq_seqs'] = seqs
        #TODO: need to handle line_dict['flankstrand_vals'] (self.flankseqcols_strand) 

    def fill_probeseqs(self,line_arr,line_dict):
        seqs_to_use = self.choose_flankseq(line_arr,self.probseq_coln,False)
        cols_used = [self.probseq_cols[i] for i in seqs_to_use]
        coln_used = [self.probseq_coln[i] for i in seqs_to_use]
        seqs = [line_arr[i] for i in coln_used]
        line_dict['probseq_colnames'] = cols_used
        line_dict['probseq_seqs'] = seqs
        #line_dict['probestrand_vals']

    def fill_general(self,line_arr,line_dict):
        snp_id = self.getrs(line_arr[self.col_unique_id])
        if self.col_dbSNP_id:
            possible = line_arr[self.col_dbSNP_id]
            if possible.startswith('rs'):
                snp_id = possible
        line_dict['snp_id'] = snp_id
        main_id = line_arr[self.col_unique_id]
        if main_id != snp_id:
            line_dict['uid'] = main_id
        line_dict['chr'] = line_arr[self.col_chr]
        line_dict['pos'] = int(line_arr[self.col_GRCh37_pos]) if self.col_GRCh37_pos else int(line_arr[self.col_GRCh38_pos])

class InfCorEx24v1a1(ChipReader):
    # test excerpt: /mnt/HPC/processed/mr875/tasks/dsp367/corev1_0_rsEg.csv
    # location: /mnt/HPC/processed/Metadata/variant_annotation/CoreExomev1.0_annotation.csv

    def __init__(self,fname,delim=','):
        super().__init__(fname,delim)
        self.GRCh37='37'
        self.datasource = "170"

    def load_cols(self):
        self.col_unique_id = self.colnum('SNP_Name') 
        self.col_chr = self.colnum('Chr')
        self.col_GRCh37_pos = self.colnum('Coord')

    def load_custom(self):
        self.flankseqcols=["Forward_Seq","Design_Seq","Top_Seq","Plus_Seq"]
        self.flankseqcoln = self.fillcust(self.flankseqcols)

    def proc_line(self,line_arr): 
        line_dict = dict()
        self.fill_flankseqs(line_arr,line_dict)
        self.fill_general(line_arr,line_dict) 
        return line_dict

class InfCorEx24v1_1a1(InfCorEx24v1a1):
    # test excerpt: /mnt/HPC/processed/mr875/tasks/dsp367/corev1_1_rsEg.csv
    # location: /mnt/HPC/processed/Metadata/variant_annotation/CoreExomev1.1_annotation.csv
    def __init__(self,fname,delim=','):
        super().__init__(fname,delim)
        self.datasource = "171"

    def load_custom(self):
        self.flankseqcols=["Forward_Seq","Design_Seq","Top_Seq"]
        self.flankseqcoln = self.fillcust(self.flankseqcols)
        
class InfImmun24v2(InfCorEx24v1_1a1):
    # test excerpt: /mnt/HPC/processed/mr875/tasks/dsp367/infimmun_Eg.csv
    # location: /mnt/HPC/processed/Metadata/variant_annotation/InfiniumImmunoArray_annotation.csv
    def __init__(self,fname):
        super().__init__(fname,"\t")
        self.datasource = "70"

class InfEx24v1a2(ChipReader):
    # test excerpt: /mnt/HPC/processed/mr875/tasks/dsp367/InfiniumExome-24v1-0_A2_Eg.csv
    # location: /mnt/HPC/processed/Metadata/variant_annotation_grch38/InfiniumExome-24v1-0_A2.csv
    def __init__(self,fname):
        super().__init__(fname)
        self.GRCh38='38'
        self.datasource="233"

    def load_cols(self):
        self.col_unique_id = self.colnum('Name')
        self.col_chr = self.colnum('Chr')
        self.col_GRCh38_pos = self.colnum('MapInfo')

    def load_custom(self):
        self.title_topgenseq = "TopGenomicSeq"
        self.title_sourceseq = "SourceSeq"
        self.col_flank_strand = self.colnum('SourceStrand')
        self.flankseqcols = [self.title_sourceseq, self.title_topgenseq]
        self.flankseqcoln = self.fillcust(self.flankseqcols)
        self.probseq_cols = ['AlleleA_ProbeSeq','AlleleB_ProbeSeq']
        self.probseq_coln = self.fillcust(self.probseq_cols)

    def proc_line(self,line_arr):
        line_dict = dict()
        self.fill_flankseqs(line_arr,line_dict)
        flankseq_colnames = line_dict['flankseq_colnames']
        flankstrand_vals = []
        for colname in flankseq_colnames:
            if colname == self.title_topgenseq:
                flankstrand_vals.append("TOP")
            elif colname == self.title_sourceseq:
                flankstrand_vals.append(line_arr[self.col_flank_strand])
            else:
                raise StrandError("can't get strand value for %s" % (colname))
        line_dict['flankstrand_vals'] = flankstrand_vals
        self.fill_probeseqs(line_arr,line_dict)
        self.fill_general(line_arr,line_dict)
        return line_dict

class InfImmun24v2grc38(InfEx24v1a2):
    # test excerpt: /mnt/HPC/processed/mr875/tasks/dsp367/infimmung38_Eg.csv
    # location: /mnt/HPC/processed/Metadata/variant_annotation_grch38/InfiniumImmunoArray-24v2-0_A2.csv
    def __init__(self,fname):
        super().__init__(fname)
        self.GRCh38='38'
        self.datasource="234"
    
class InfCorEx24v1_1grc38(InfEx24v1a2):
    # test excerpt: /mnt/HPC/processed/mr875/tasks/dsp367/infincorex38_Eg.csv
    # location: /mnt/HPC/processed/Metadata/variant_annotation_grch38/InfiniumCoreExome-24v1-1_A2.csv
    def __init__(self,fname):
        super().__init__(fname)
        self.GRCh38='38'
        self.datasource="232"

class Dil(ChipReader):
    # example excerpt: /mnt/HPC/processed/mr875/tasks/dsp367/DIL_annotation_Eg.csv 
    # original file: /mnt/HPC/processed/Metadata/variant_annotation/DIL_annotation.csv

    def __init__(self,fname):
        super().__init__(fname,"\t")
        self.GRCh37='37'
        self.datasource="114"

    def load_cols(self):
        self.col_unique_id = self.colnum('label')
        self.col_dbSNP_id = self.colnum('dbSNP')
        self.col_chr = self.colnum('chromosome')
        self.col_GRCh37_pos = self.colnum('start')

    def load_custom(self):
        self.col_flank_strand = self.colnum("strand")
        self.flankseqcols = ["5 prime","observed","3 prime"]
        self.flankseqcoln = self.fillcust(self.flankseqcols)

    def proc_line(self,line_arr):
        line_dict = dict()
        line_dict['flankseq'] = "".join([line_arr[coln] for coln in self.flankseqcoln])
        line_dict['flankseq_colname'] = "3_col_merge"
        strand = line_arr[self.col_flank_strand].replace('-1','-').replace('1','+')
        line_dict['flankstrand_val'] = strand
        line_dict['snp_id'] = None
        dbsnpid = line_arr[self.col_dbSNP_id] # empty column delivers None type
        main_id = line_arr[self.col_unique_id]
        if dbsnpid:
            line_dict['snp_id'] = dbsnpid
        if dbsnpid != main_id:
            line_dict['uid'] = main_id
        line_dict['pos'] = int(line_arr[self.col_GRCh37_pos])
        chrom = line_arr[self.col_chr]
        line_dict['chr'] = chrom.replace('Hs','')
        return line_dict

class AxiUKBBAffy2_1(ChipReader):
    # example excerpt: /mnt/HPC/processed/mr875/tasks/dsp367/AxiUKBBAffy2_1_38_Eg.csv
    # original file: /mnt/HPC/processed/mr875/tasks/dsp367/Axiom_UKBBv2_1.na36.r1.a1.annot.csv

    
    def __init__(self,fname):
        super().__init__(fname)
        self.GRCh38='38'
        self.datasource="231"

    def load_cols(self):
        while self.header[0].startswith('#'):
            self.header = next(self.it)
        self.col_unique_id = self.colnum('Affy SNP ID')
        self.col_dbSNP_id = self.colnum('dbSNP RS ID')
        self.extcol_dbSNP_id = self.colnum('Extended RSID')
        self.col_chr = self.colnum('Chromosome')
        self.col_GRCh38_pos = self.colnum('Physical Position')

    def load_custom(self):
        self.col_flank_strand = self.colnum('Strand')
        self.tit_flank_seq = 'Flank'
        self.col_flank_seq = self.colnum(self.tit_flank_seq)

    def proc_line(self,line_arr):
        line_dict = dict()
        firstdbsnp = line_arr[self.col_dbSNP_id]
        secdbsnp = line_arr[self.extcol_dbSNP_id]
        self.fill_flankseqs(line_arr,line_dict,True)
        self.fill_general(line_arr,line_dict)
        if not firstdbsnp.startswith('rs'): # null values look like '---'
            if secdbsnp.startswith('rs'):
                line_dict['snp_id'] = secdbsnp
        return line_dict

class AxiUKBB_WCSG(ChipReader):
    # example excerpt: /mnt/HPC/processed/mr875/tasks/dsp367/AxiUKBB_WCSG_Eg.csv
    # original file: /mnt/HPC/processed/Metadata/variant_annotation/Axiom_UKB_WCSG.na35.annot-2015.csv

    def __init__(self,fname):
        super().__init__(fname)
        self.GRC37='37'
        self.datasource="999" #temporary

    def load_cols(self):
        while self.header[0].startswith('#'):
            self.header = next(self.it)
        self.col_unique_id = self.colnum('Affy SNP ID')
        self.col_dbSNP_id = self.colnum('dbSNP RS ID')
        self.col_chr = self.colnum('Chromosome')
        self.col_GRCh37_pos = self.colnum('Physical Position')

    def load_custom(self):
        self.col_flank_strand = self.colnum('Strand')
        self.tit_flank_seq = 'Flank'
        self.col_flank_seq = self.colnum(self.tit_flank_seq)

    def proc_line(self,line_arr):
        line_dict = dict()
        self.fill_flankseqs(line_arr,line_dict,True)
        self.fill_general(line_arr,line_dict)
        return line_dict

