import mysql.connector
import time
import datetime
import logging
from subprocess import check_output
from varianti import VariantI
from chipreader import *
from connect import DBConnect

def readin(chip,reader):
    orig_file = reader.fname
    line_count = int(check_output(["wc", "-l", orig_file]).split()[0])
    variant_count = 1
    start = time.time()
    logging.info('file: %s',(orig_file))
    fvper = int(0.05 * line_count)
    logline = fvper
    curs = chip.getCursor()
    build = "38"
    if reader.col_GRCh37_pos:
        build = "37"
    new_ds = reader.datasource
    prev_id = "first"
    for line in reader.linebyline():
        if logline == variant_count:
            now = int(time.time() - start)
            logging.info("approximately %.2f%% parsed after %s seconds, %s variants" % ((variant_count/line_count*100),now,variant_count-1))
            logline += fvper
        try:
            dic = reader.proc_line(line)
        except IndexError:
            statement = "IndexError when trying to process line (reader.proc_line).\nProbably this line doesn't have all expected columns:\n"+ "--".join(line) + "\nskipping this line"
            logging.warning(statement)
            continue
        except ValueError as ve:
            statement = "ValueError thrown (reader.proc_line).\nprobably from int conversion of ukbb style '---' reference positions. Last variation parsed was {}. Error: {}\nskipping this variant".format(prev_id,ve)
            logging.warning(statement)
            continue
        else:
            prev_id = dic['uid']
        try:
            variant = VariantI(curs,dic,new_ds,build)
            variant.log_flank()
            variant.log_probe()
            variant.log_coord()
        except mysql.connector.errors.DataError as de:
            mess = "error thrown, suspected long chromosome name ({}). skipping {}/{}: {}".format(dic['chr'],dic['uid'],dic['snp_id'],de)
            logging.warning(mess)
            chip.rollback()
        except Exception as e:
            mess = "error with {}/{}".format(dic['uid'],dic['snp_id'])
            logging.error(mess)
            print(mess)
            chip.rollback()
            raise
        else:
            chip.commit()
            variant_count += 1
    now = int(time.time() - start)
    logging.info('Finished: %s seconds passed, %s variants' % (now,(variant_count-1)))

#readers = [InfCorEx24v1a1('/mnt/HPC/processed/mr875/tasks/dsp367/corev1_0_rsEg.csv'),
#        InfEx24v1a2('/mnt/HPC/processed/mr875/tasks/dsp367/InfiniumExome-24v1-0_A2_Eg.csv'),
#        Dil('/mnt/HPC/processed/mr875/tasks/dsp367/DIL_annotation_Eg.csv'),
#       InfCorEx24v1_1a1('/mnt/HPC/processed/mr875/tasks/dsp367/corev1_1_rsEg.csv'),
#           AxiUKBBAffy2_1('/mnt/HPC/processed/mr875/tasks/dsp367/AxiUKBBAffy2_1_38_Eg.csv'),
        #InfImmun24v2('/mnt/HPC/processed/mr875/tasks/dsp367/infimmun_Eg.csv')]
# debug:
#readers = [AxiUKBBAffy2_1('/mnt/HPC/processed/mr875/tasks/dsp367/AxiUKBBAffy2_1_38_Eg.csv')]


#readers = [InfCorEx24v1a1('/mnt/HPC/processed/Metadata/variant_annotation/CoreExomev1.0_annotation.csv')]
#readers = [InfEx24v1a2('/mnt/HPC/processed/Metadata/variant_annotation_grch38/InfiniumExome-24v1-0_A2.csv')]
#readers = [InfCorEx24v1_1a1('/mnt/HPC/processed/Metadata/variant_annotation/CoreExomev1.1_annotation.csv')]

readers = [AxiUKBBAffy2_1('/mnt/HPC/processed/mr875/tasks/dsp367/Axiom_UKBBv2_1.na36.r1.a1.annot.csv')]
#readers = [Dil('/mnt/HPC/processed/Metadata/variant_annotation/DIL_annotation.csv')]
#readers = [InfImmun24v2('/mnt/HPC/processed/Metadata/variant_annotation/InfiniumImmunoArray_annotation.csv')]


ch = DBConnect("cc2")
#ch = DBConnect("chip_comp")
logfile = datetime.datetime.now().strftime("%a_%d%b_%I%p.log")
logging.basicConfig(filename=logfile, level=logging.INFO)
for source in readers:
    readin(ch,source)
ch.close()


