from chipreader import InfCorEx24v1a1

reader = InfCorEx24v1a1('/mnt/HPC/processed/mr875/tasks/dsp367/corev1_0_rsEg.csv')
print(reader.flankseqcols,reader.flankseqcoln)
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

