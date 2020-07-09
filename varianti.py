class VariantI:

    def __init__(self,curs,row_dic,ds):
        self.dic = row_dic
        self.secondary_id = None
        self.main_id = None
        self.datasource = ds
        if 'uid' in self.dic:
            self.secondary_id = self.dic['uid']
        self.dbsnpid = self.dic['snp_id']
        self.curs = curs
        if self.dbsnpid:
            self.log_dbsnpid()
        else:
            self.log_secid()
        self.snp_present(self.main_id,self.datasource)

    def id_exists(self,uid):
        q = "SELECT uid_datasource FROM consensus WHERE id = %s"
        self.curs.execute(q,(uid,))
        initial_ds = self.curs.fetchone()
        return initial_ds

    def altid_exists(self,uid):
        q = "SELECT id from alt_ids where alt_id = %s"
        self.curs.execute(q,(uid,))
        alt_id_exists = self.curs.fetchall()
        if alt_id_exists:
            return alt_id_exists[0]
        else:
            return None

    def add_primary(self,prim_id,dsource,sec=None):
        self.curs.execute("INSERT INTO consensus (id,uid_datasource) VALUES (%s, %s)",(prim_id,dsource))
        if sec:
            self.curs.execute("INSERT INTO alt_ids (id, alt_id, datasource) VALUES (%s,%s,%s)",(prim_id,sec,dsource))

    def add_altid(self,prim_id,sec_id,new_ds):
        self.curs.execute("SELECT EXISTS(SELECT * FROM alt_ids WHERE id = %s AND alt_id = %s AND datasource = %s)",(prim_id,sec_id,new_ds))
        if not self.curs.fetchone()[0]:
            curs.execute("INSERT INTO alt_ids (id, alt_id, datasource) VALUES (%s, %s, %s)",(prim_id,sec_id,new_ds))

    def snpid_swapin(self,uid_to_swapout,db_snp,old_ds,new_ds,this_altid=None):
        #swap dbsnp id into consensus table, put initial id into alt_ids
        self.curs.execute("UPDATE consensus SET id = %s, uid_datasource = %s where id = %s",(db_snp,new_ds,uid_to_swapout))
        self.curs.execute("UPDATE alt_ids SET id = %s where id = %s",(db_snp,uid_to_swapout))
        self.curs.execute("UPDATE flank SET id = %s where id = %s",(db_snp,uid_to_swapout))
        self.curs.execute("UPDATE positions SET id = %s where id = %s",(db_snp,uid_to_swapout))
        self.curs.execute("UPDATE probes SET id = %s where id = %s",(db_snp,uid_to_swapout))
        self.curs.execute("UPDATE snp_present SET id = %s where id = %s",(db_snp,uid_to_swapout))
        self.curs.execute("INSERT INTO alt_ids (id, alt_id, datasource) VALUES (%s, %s, %s)",(db_snp,uid_to_swapout,old_ds))
        if this_altid:
            self.curs.execute("INSERT INTO alt_ids (id, alt_id, datasource) VALUES (%s, %s, %s)",(db_snp,this_altid,new_ds))

    def snp_present(self,uid,datasource):
        self.curs.execute("INSERT IGNORE INTO snp_present (id,datasource) VALUES (%s,%s)",(uid,datasource))

    def log_dbsnpid(self):
        self.main_id = self.dbsnpid
        snp_exist = self.id_exists(self.dbsnpid)
        if snp_exist:
            if self.secondary_id: #snpid found but if sec id is new, add to alt_ids
                self.add_altid(self.dbsnpid,self.secondary_id,new_ds=self.datasource)
        elif self.secondary_id: #snpid not in consensus but if the uid contains a non dbsnp id
            uid_to_swapout = self.secondary_id # and it is in consensus table, it needs swapping
            uid_exist = self.id_exists(uid=uid_to_swapout)
            if uid_exist:
                old_ds = uid_exist[0]
                #swap dbsnp id into consensus table, put initial value and datasource into alt_ids 
                self.snpid_swapin(uid_to_swapout,self.dbsnpid,old_ds,new_ds=self.datasource)
            else:
                diff_uid = self.altid_exists(self.secondary_id)
                if diff_uid: # if snp exists in consensus under different id
                    uid_to_swapout = diff_uid[0]
                    old_ds = id_exists(uid_to_swapout)[0]
                    #swap dbsnp id into consensus table, put initial value and ds into alt_ids AND add this datasource as new alt_id
                    self.snpid_swapin(uid_to_swapout,self.dbsnpid,old_ds,new_ds=self.datasource,this_altid=self.secondary_id)
                else: # alternative not found anywhere
                    #add snp id to consensus and secondary to alt
                    self.add_primary(self.dbsnpid,self.datasource,sec=self.secondary_id)
        else: # not found and no alternative id
            #add dbsnp to consensus and nothing to alt
            self.add_primary(self.dbsnpid,self.datasource)
        
    def log_secid(self):
        self.main_id = self.secondary_id
        uid_ds = self.id_exists(self.secondary_id)
        if not uid_ds: # secondary id not in consensus table
            altid = self.altid_exists(self.secondary_id)
            if altid: # but it is listed in the alt_ids table so get the primary id from there
                self.main_id = altid[0]
            else: # not in alt_ids table either so add secondary_id to consensus
                self.add_primary(self.secondary_id,self.datasource)
        #else: secondary id already in consensus table, main_id already switched

