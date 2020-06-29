#from chipreader import ChipReader
from chipreader import InfCorEx24v1a1


reader = InfCorEx24v1a1('/mnt/HPC/processed/mr875/tasks/dsp367/corev1_0_rsEg.csv')
#reader = InfCorEx24v1a1('/mnt/HPC/processed/Metadata/variant_annotation/CoreExomev1.0_annotation.csv')
#reader = InfCorEx24v1a1('/mnt/HPC/processed/Metadata/variant_annotation/LGC_annotation.csv')
for l in reader.linebyline(15):
    print(l[1],reader.getrs(l[1]))

print(reader.datasource)
print(reader.col_unique_id)
print(reader.mcols_flank_seq)

try:
    next(reader.it)
except StopIteration:
    pass

for l in reader.linebyline(1):
    print(l)

