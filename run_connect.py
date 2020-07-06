from chipreader import *
from connect import DBConnect

def dbsnp_exists(curs,snp_id):
    #q = "SELECT EXISTS(select * from consensus where id = %s)"
    q = "SELECT chr, GRCh37_pos, GRCh38_pos FROM consensus WHERE id = %s"
    curs.execute(q,(snp_id,))
    snp_exists = curs.fetchone()
    return snp_exists

def uid_exists(curs,uid):
    pass

def compcons_pos(chrm,gr37,gr38):
    pass

reader = InfCorEx24v1a1('/mnt/HPC/processed/mr875/tasks/dsp367/corev1_0_rsEg.csv')
chip = DBConnect("chip_comp")
curs = chip.getCursor()
for line in reader.linebyline():
    dic = reader.proc_line(line)
    if dic['snp_id']:
        con_snp = dbsnp_exists(curs,snp_id = dic['snp_id'])
        if con_snp:
            compcons_pos(chrm=con_snp[0],gr37=con_snp[1],gr38=con_snp[2])
        elif dic['uid'] != dic['snp_id']:
            con_uid = uid_exists(curs,uid = dic['uid'])

#    print(dic)
    
chip.close()

#query2 = ("show tables")
#chipcursor=chip.getCursor()
#chipcursor.execute(query2)
#for row in chipcursor.fetchall():
#    print(row)
#chip.close()

