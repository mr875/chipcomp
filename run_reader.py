#from chipreader import ChipReader
from chipreader import InfCorEx24v1a1

reader = InfCorEx24v1a1('/mnt/HPC/processed/Metadata/variant_annotation/CoreExomev1.0_annotation.csv')
#reader = InfCorEx24v1a1('/mnt/HPC/processed/Metadata/variant_annotation/LGC_annotation.csv')
for l in reader.linebyline(15):
    print(l[0])

print(reader.delim)

try:
    next(reader.it)
except StopIteration:
    pass

for l in reader.linebyline(1):
    print(l)

