from chipreader import InfCorEx24v1a1

reader = InfCorEx24v1a1('/mnt/HPC/processed/mr875/tasks/dsp367/corev1_0_rsEg.csv')
for l in reader.linebyline(2):
    reader.proc_line(l)
#    print(l[1],reader.getrs(l[1]))

try:
    next(reader.it)
except StopIteration:
    pass

for l in reader.linebyline(1):
    pass
    #print(l)

