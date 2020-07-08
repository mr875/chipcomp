from chipreader import *

#reader = InfCorEx24v1a1('/mnt/HPC/processed/mr875/tasks/dsp367/newcore.csv')
reader = InfCorEx24v1a1('/mnt/HPC/processed/mr875/tasks/dsp367/corev1_0_rsEg.csv')
print(reader.flankseqcols,reader.flankseqcoln)
#reader = InfEx24v1a2('/mnt/HPC/processed/mr875/tasks/dsp367/InfiniumExome-24v1-0_A2_Eg.csv')
#print(reader.flankseqcols, reader.flankseqcoln,reader.probseq_cols,reader.probseq_coln)
for l in reader.linebyline():
    line_dict = reader.proc_line(l)
    print(line_dict)
    print()


try:
    next(reader.it)
except StopIteration:
    pass

for l in reader.linebyline(1):
    pass
    #print(l)

