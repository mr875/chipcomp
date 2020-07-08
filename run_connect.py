from chipreader import *
from connect import DBConnect

def id_exists(curs,uid):
    #q = "SELECT EXISTS(select * from consensus where id = %s)"
    q = "SELECT uid_datasource FROM consensus WHERE id = %s"
    curs.execute(q,(uid,))
    id_exists = curs.fetchone()
    return id_exists

def altid_exists(curs,uid):
    q = "SELECT id from alt_ids where alt_id = %s"
    curs.execute(q,(uid,))
    alt_id_exists = curs.fetchall()
    if alt_id_exists:
        return alt_id_exists[0]
    else:
        return None

def add_primary(curs,prim_id,dsource,sec=None):
    curs.execute("INSERT INTO consensus (id,uid_datasource) VALUES (%s, %s)",(prim_id,dsource))
    if sec:
        curs.execute("INSERT INTO alt_ids (id, alt_id, datasource) VALUES (%s,%s,%s)",(prim_id,sec,dsource))

def add_altid(curs,prim_id,sec_id,new_ds):
    curs.execute("SELECT EXISTS(SELECT * FROM alt_ids WHERE id = %s AND alt_id = %s AND datasource = %s)",(prim_id,sec_id,new_ds))
    if not curs.fetchone()[0]:
        curs.execute("INSERT INTO alt_ids (id, alt_id, datasource) VALUES (%s, %s, %s)",(prim_id,sec_id,new_ds))

def snpid_swapin(curs,uid_to_swapout,db_snp,old_ds,new_ds,this_altid=None):
    #swap dbsnp id into consensus table, put initial id into alt_ids
    curs.execute("UPDATE consensus SET id = %s, uid_datasource = %s where id = %s",(db_snp,new_ds,uid_to_swapout))
    curs.execute("UPDATE alt_ids SET id = %s where id = %s",(db_snp,uid_to_swapout))
    curs.execute("UPDATE flank SET id = %s where id = %s",(db_snp,uid_to_swapout))
    curs.execute("UPDATE positions SET id = %s where id = %s",(db_snp,uid_to_swapout))
    curs.execute("UPDATE probes SET id = %s where id = %s",(db_snp,uid_to_swapout))
    curs.execute("UPDATE snp_present SET id = %s where id = %s",(db_snp,uid_to_swapout))
    curs.execute("INSERT INTO alt_ids (id, alt_id, datasource) VALUES (%s, %s, %s)",(db_snp,uid_to_swapout,old_ds))
    if this_altid:
        curs.execute("INSERT INTO alt_ids (id, alt_id, datasource) VALUES (%s, %s, %s)",(db_snp,this_altid,new_ds))

def compcons_pos(chrm,gr37,gr38):
    pass

#reader = InfCorEx24v1a1('/mnt/HPC/processed/mr875/tasks/dsp367/corev1_0_rsEg.csv')
#reader = InfCorEx24v1a1('/mnt/HPC/processed/mr875/tasks/dsp367/corev1_0_rsEg.csv')
reader = InfEx24v1a2('/mnt/HPC/processed/mr875/tasks/dsp367/InfiniumExome-24v1-0_A2_Eg.csv')

chip = DBConnect("chip_comp")
curs = chip.getCursor()
new_ds = reader.datasource
for line in reader.linebyline():
    dic = reader.proc_line(line)
    dbsnpid = dic['snp_id']
    secondary_id = None
    main_id = None
    if 'uid' in dic:
        secondary_id = dic['uid']
    if dbsnpid:
        main_id = dbsnpid
        snp_exist = id_exists(curs,uid = dbsnpid)
        if snp_exist:
            if secondary_id: #snpid found but if sec id is new add to alt_ids
                add_altid(curs,dbsnpid,secondary_id,new_ds)
                chip.commit()
        elif secondary_id: #snpid not in consensus but if the uid contains a non dbsnp id
            uid_to_swapout = secondary_id # and it is in consensus table, it needs swapping
            uid_exist = id_exists(curs,uid=uid_to_swapout)
            if uid_exist:
                old_ds = uid_exist[0]
                #swap dbsnp id into consensus table, put initial value and datasource into alt_ids 
                snpid_swapin(curs,uid_to_swapout,dbsnpid,old_ds,new_ds)
                chip.commit()
            else:
                diff_uid = altid_exists(curs,uid = secondary_id)
                if diff_uid: # if snp exists in consensus under different id
                    uid_to_swapout = diff_uid[0]
                    old_ds = id_exists(curs,uid_to_swapout)[0]
                    #swap dbsnp id into consensus table, put initial value and ds into alt_ids AND add this datasource as new alt_id
                    snpid_swapin(curs,uid_to_swapout,dbsnpid,old_ds,new_ds,this_altid=secondary_id)
                    chip.commit()
                else: # alternative not found anywhere
                    #add snp id to consensus and secondary to alt
                    add_primary(curs,dbsnpid,new_ds,sec=secondary_id)
                    chip.commit()
        else: # not found and no alternative id
            #add dbsnp to consensus and nothing to alt
            add_primary(curs,dbsnpid,new_ds)
    else: #no dbsnp available
        main_id = secondary_id
        uid_ds = id_exists(curs,secondary_id)
        if not uid_ds: # secondary id not in consensus table
            altid = altid_exists(curs,secondary_id)
            if altid: # but it is listed in the alt_ids table so get the primary id from there
                main_id = altid[0]
            else: # not in alt_ids table either so add secondary_id to consensus
                add_primary(curs,secondary_id,new_ds)
                chip.commit()
        #else: secondary id already in consensus table, main_id already switched

chip.close()


#query2 = ("show tables")
#chipcursor=chip.getCursor()
#chipcursor.execute(query2)
#for row in chipcursor.fetchall():
#    print(row)
#chip.close()

