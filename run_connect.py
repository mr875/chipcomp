from varianti import VariantI
from chipreader import *
from connect import DBConnect

def readin(chip,reader):
    curs = chip.getCursor()
    build = "38"
    if reader.col_GRCh37_pos:
        build = "37"
    new_ds = reader.datasource
    for line in reader.linebyline():
        dic = reader.proc_line(line)
        dbsnpid = dic['snp_id']
        variant = VariantI(curs,dic,new_ds,build)
#        chip.commit()
        variant.log_flank()
#        chip.commit()
        variant.log_probe()
#        chip.commit()
        variant.log_coord()
        chip.commit()

readers = [InfCorEx24v1a1('/mnt/HPC/processed/mr875/tasks/dsp367/corev1_0_rsEg.csv'),
        InfEx24v1a2('/mnt/HPC/processed/mr875/tasks/dsp367/InfiniumExome-24v1-0_A2_Eg.csv')]

ch = DBConnect("chip_comp")
for source in readers:
    readin(ch,source)
ch.close()


