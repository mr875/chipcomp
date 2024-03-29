from chipreader import *

''' small excerpt tests
readers = [InfCorEx24v1a1('/mnt/HPC/processed/mr875/tasks/dsp367/corev1_0_rsEg.csv'),
    InfEx24v1a2('/mnt/HPC/processed/mr875/tasks/dsp367/InfiniumExome-24v1-0_A2_Eg.csv'),
    Dil('/mnt/HPC/processed/mr875/tasks/dsp367/DIL_annotation_Eg.csv'),
    AxiUKBBAffy2_1('/mnt/HPC/processed/mr875/tasks/dsp367/AxiUKBBAffy2_1_38_Eg.csv'),
    InfImmun24v2('/mnt/HPC/processed/mr875/tasks/dsp367/infimmun_Eg.csv'),
    AxiUKBB_WCSG('/mnt/HPC/processed/mr875/tasks/dsp367/AxiUKBB_WCSG_Eg.csv'),
    InfImmun24v2grc38('/mnt/HPC/processed/mr875/tasks/dsp367/infimmung38_Eg.csv'),
    InfCorEx24v1_1grc38('/mnt/HPC/processed/mr875/tasks/dsp367/infincorex38_Eg.csv'),
    InfOmniExpr('/mnt/HPC/processed/mr875/tasks/dsp367/infomniexpr_Eg.csv'),
    InfOmniExpr38('/mnt/HPC/processed/mr875/tasks/dsp367/infomniexpr38_Eg.csv'),
    MSExome('/mnt/HPC/processed/mr875/tasks/dsp367/msexome_Eg.csv')]
'''
readers = [UKBBv21_2021('ukbbv2_1_Annot_2021.csv')]

for reader in readers:
#    if not type(reader).__name__ == "MSExome":
#        continue
    print(reader.header)
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

