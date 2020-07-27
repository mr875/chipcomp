from chipreader import *

#reader = InfCorEx24v1a1('/mnt/HPC/processed/mr875/tasks/dsp367/corev1_0_rsEg.csv')
#reader = InfEx24v1a2('/mnt/HPC/processed/mr875/tasks/dsp367/InfiniumExome-24v1-0_A2_Eg.csv')
#reader = Dil('/mnt/HPC/processed/mr875/tasks/dsp367/DIL_annotation_Eg.csv')
#reader = InfCorEx24v1_1a1('/mnt/HPC/processed/mr875/tasks/dsp367/corev1_1_rsEg.csv')
readers = [InfCorEx24v1a1('/mnt/HPC/processed/mr875/tasks/dsp367/corev1_0_rsEg.csv'),
    InfEx24v1a2('/mnt/HPC/processed/mr875/tasks/dsp367/InfiniumExome-24v1-0_A2_Eg.csv'),
    Dil('/mnt/HPC/processed/mr875/tasks/dsp367/DIL_annotation_Eg.csv'),
    AxiUKBBAffy2_1('/mnt/HPC/processed/mr875/tasks/dsp367/AxiUKBBAffy2_1_38_Eg.csv'),
    InfImmun24v2('/mnt/HPC/processed/mr875/tasks/dsp367/infimmun_Eg.csv'),
    AxiUKBB_WCSG('/mnt/HPC/processed/mr875/tasks/dsp367/AxiUKBB_WCSG_Eg.csv'),
    InfImmun24v2grc38('/mnt/HPC/processed/mr875/tasks/dsp367/infimmung38_Eg.csv')]

for reader in readers:
#    print(reader.header)
#    if not type(reader).__name__ == "InfImmun24v2grc38":
#        continue
    for l in reader.linebyline(3):
        line_dict = reader.proc_line(l)
        print(line_dict)
    print()


#try:
#    next(reader.it)
#except StopIteration:
#    pass
#
#for l in reader.linebyline(1):
#    pass
    #print(l)

