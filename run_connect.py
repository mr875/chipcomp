import mysql.connector
import time
import datetime
import logging
from subprocess import check_output
from varianti import VariantI
from chipreader import *
from connect import DBConnect

def readin(chip,reader,report_mode,offset=0):
    orig_file = reader.fname
    line_count = int(check_output(["wc", "-l", orig_file]).split()[0])
    variant_count = 0
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
    for line in reader.linebyline(): # remove int when testing done
        variant_count+=1
        if logline == variant_count:
            now = int(time.time() - start)
            logging.info("approximately %.2f%% parsed after %s seconds, %s variants" % ((variant_count/line_count*100),now,variant_count-1))
            logline += fvper
        if variant_count < offset:
            continue
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
        except:
            logging.error('uknown error at reader.proc_line(line)')
            ch.close()
            raise
        else:
            if 'uid' in dic:
                prev_id = dic['uid']
            else:
                prev_id = dic['snp_id']
        try:
            variant = VariantI(curs,dic,new_ds,build,report_mode)
            variant.log_flank()
            variant.log_probe()
            variant.log_coord()
        except mysql.connector.errors.DataError as de:
            mess = "sql DataError thrown, suspected long chromosome name ({}). skipping {}: {}".format(dic['chr'],prev_id,de)
            logging.warning(mess)
            chip.rollback()
        except mysql.connector.errors.IntegrityError as ine:
            mess = "sql IntegrityError thrown.\ncould be from duplicate entry of primary key into flank table.\nskipping {}:{}".format(prev_id,ine)
            logging.warning(mess)
            chip.rollback()
        except Exception as e:
            mess = "unknown error with {}".format(prev_id)
            logging.error(mess)
            print(mess)
            chip.rollback()
            ch.close()
            raise
        else:
            chip.commit()
    now = int(time.time() - start)
    logging.info('Finished: %s seconds passed, %s variants' % (now,(variant_count)))

#readers = [InfCorEx24v1a1('/mnt/HPC/processed/mr875/tasks/dsp367/corev1_0_rsEg.csv'),
#        InfEx24v1a2('/mnt/HPC/processed/mr875/tasks/dsp367/InfiniumExome-24v1-0_A2_Eg.csv'),
#        Dil('/mnt/HPC/processed/mr875/tasks/dsp367/DIL_annotation_Eg.csv'),
#       InfCorEx24v1_1a1('/mnt/HPC/processed/mr875/tasks/dsp367/corev1_1_rsEg.csv'),
#           AxiUKBBAffy2_1('/mnt/HPC/processed/mr875/tasks/dsp367/AxiUKBBAffy2_1_38_Eg.csv'),
#        InfImmun24v2('/mnt/HPC/processed/mr875/tasks/dsp367/infimmun_Eg.csv'),
#        AxiUKBB_WCSG('/mnt/HPC/processed/mr875/tasks/dsp367/AxiUKBB_WCSG_Eg.csv'),
#        InfImmun24v2grc38('/mnt/HPC/processed/mr875/tasks/dsp367/infimmung38_Eg.csv'), 
#        InfCorEx24v1_1grc38('/mnt/HPC/processed/mr875/tasks/dsp367/infincorex38_Eg.csv'),
#        InfOmniExpr('/mnt/HPC/processed/mr875/tasks/dsp367/infomniexpr_Eg.csv'),
#        InfOmniExpr38('/mnt/HPC/processed/mr875/tasks/dsp367/infomniexpr38_Eg.csv')]
#readers = [MSExome('/mnt/HPC/processed/mr875/tasks/dsp367/msexome_Eg.csv')]
# debug:
#readers = [AxiUKBBAffy2_1('/mnt/HPC/processed/mr875/tasks/dsp367/AxiUKBBAffy2_1_38_Eg.csv')]

#readers = [InfCorEx24v1a1('/mnt/HPC/processed/Metadata/variant_annotation/CoreExomev1.0_annotation.csv'),
#        InfEx24v1a2('/mnt/HPC/processed/Metadata/variant_annotation_grch38/InfiniumExome-24v1-0_A2.csv'),
#        InfCorEx24v1_1a1('/mnt/HPC/processed/Metadata/variant_annotation/CoreExomev1.1_annotation.csv'),
#        AxiUKBBAffy2_1('/mnt/HPC/processed/mr875/tasks/dsp367/Axiom_UKBBv2_1.na36.r1.a1.annot.csv'),
#        AxiUKBB_WCSG('/mnt/HPC/processed/Metadata/variant_annotation/Axiom_UKB_WCSG.na35.annot-2015.csv'),
#        InfImmun24v2('/mnt/HPC/processed/Metadata/variant_annotation/InfiniumImmunoArray_annotation.csv'),
#        InfImmun24v2grc38('/mnt/HPC/processed/Metadata/variant_annotation_grch38/InfiniumImmunoArray-24v2-0_A2.csv'),
#        InfCorEx24v1_1grc38('/mnt/HPC/processed/Metadata/variant_annotation_grch38/InfiniumCoreExome-24v1-1_A2.csv'),
#        InfOmniExpr('/mnt/HPC/processed/Metadata/variant_annotation/OmniExpress_annotation.csv'),
#        InfOmniExpr38('/mnt/HPC/processed/Metadata/variant_annotation_grch38/InfiniumOmniExpress-24v1-2_A2.csv'),
#        MSExome('/mnt/HPC/processed/Metadata/variant_annotation/MSExome_annotation.csv')]

#readers = [Dil('/mnt/HPC/processed/Metadata/variant_annotation/DIL_annotation.csv')]

#readers = [AxiUKBBAffy2_1('Axiom_UKBBv2_1.na36.r3.a3.annot.csv')]
readers = [AxiUKBBAffy2_1('newflank_orig_lines_valchr.txt')]

ch = DBConnect("cc2")
report_mode = True
logfile = datetime.datetime.now().strftime("%a_%d%b_%I%p.log")
logging.basicConfig(filename=logfile, level=logging.INFO)
offsetclass = "" # "AxiUKBB_WCSG"  #pick a source class from which you don't want to parse from the beginning
offsetvariant = 249200 # variant 1 = the first line under the header. so if offsetvariant = 3 then the 3rd variant will be parsed
for source in readers:
    if type(source).__name__ == offsetclass:
        readin(ch,source,report_mode,offsetvariant)
    else:
        readin(ch,source,report_mode)
ch.close()


