class VariantI:

    def __init__(self,curs,row_dic,ds,build):
        self.dic = row_dic
        self.secondary_id = None
        self.main_id = None
        self.datasource = ds
        self.build = build
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
            self.curs.execute("INSERT INTO alt_ids (id, alt_id, datasource) VALUES (%s, %s, %s)",(prim_id,sec_id,new_ds))

    def snpid_swapin(self,uid_to_swapout,db_snp,old_ds,new_ds,this_altid=None):
        #swap dbsnp id into consensus table, put initial id into alt_ids
        self.curs.execute("UPDATE consensus SET id = %s, uid_datasource = %s where id = %s",(db_snp,new_ds,uid_to_swapout))
        self.curs.execute("UPDATE alt_ids SET id = %s where id = %s",(db_snp,uid_to_swapout))
        self.curs.execute("UPDATE flank SET id = %s where id = %s",(db_snp,uid_to_swapout))
        self.curs.execute("UPDATE positions SET id = %s where id = %s",(db_snp,uid_to_swapout))
        self.curs.execute("UPDATE probes SET id = %s where id = %s",(db_snp,uid_to_swapout))
        self.curs.execute("UPDATE snp_present SET id = %s where id = %s",(db_snp,uid_to_swapout))
        self.curs.execute("UPDATE match_count SET id = %s where id = %s",(db_snp,uid_to_swapout))
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

    def flankmatch(self,thisflank,dbflank,probe=False):
        thisflank = thisflank.upper()
        dbflank = dbflank.upper()
        if not probe:
            thisflank = thisflank.split('[')[0]+thisflank.split(']')[1]
            dbflank = dbflank.split('[')[0]+dbflank.split(']')[1]
        if thisflank in dbflank:
            return 1 # new flank is shorter/equal to db version, count+1 on existing
        if dbflank in thisflank:
            return 2 # new flank is longer, swap in
        return 0  # new flank is different

    def countplus(self,searchby,table,column): #lighter version of addmatch(), will do false counts if run on the same datasource more than once
        q = "UPDATE "+table+" SET match_count=match_count+1 WHERE id = %s AND "+column+" = %s AND datasource <> %s"
        self.curs.execute(q,(self.main_id,searchby,self.datasource))

    def addmatch(self,dbflank,table,ds=None):
        if not ds:
            ds = self.datasource
        q = "INSERT IGNORE INTO match_count (id, tabl, match_value, datasource) VALUES (%s, %s, %s, %s)"
        vals = (self.main_id,table,dbflank,ds)
        self.curs.execute(q,vals) #print(q % vals) 

    def squeezeflank(self,dbflank,probe=False):
        where = (self.main_id,dbflank)
        select = "SELECT datasource "
        delete = "DELETE "
        table = "flank"
        row = "FROM flank WHERE id = %s AND flank_seq = %s"
        if probe:
            row = "FROM probes WHERE id = %s AND probe_seq = %s"
            table = "probes"
        self.curs.execute(select+row,where)
        rowlist = self.curs.fetchall()
        if not rowlist:
            return  #db entry already deleted
        oldds = rowlist[0][0]
        self.addmatch(dbflank,table,oldds)
        self.curs.execute(delete+row,where)

    def insertflank(self,thisflank,colname,fstrand,multiple,probe=False):
        q = "INSERT INTO flank (id,colname,datasource,flank_seq,flank_strand,multiple) VALUES (%s, %s, %s, %s, %s, %s)"
        if probe:
            q = "INSERT INTO probes (id,colname,datasource,probe_seq,probe_strand,multiple) VALUES (%s, %s, %s, %s, %s, %s)"
        vals = (self.main_id, colname, self.datasource,thisflank,fstrand,multiple)
        self.curs.execute(q,vals)

    def log_flank(self):
        self.curs.execute("SELECT flank_seq,datasource FROM flank WHERE id = %s",(self.main_id,))    
        indb = self.curs.fetchall() #list of tuples
        theseflanks = []
        thesecolnames = []
        multiple = False
        thesefstrand = []
        if 'flankseq_seqs' in self.dic:
            multiple = True
            theseflanks = self.dic['flankseq_seqs']
            thesecolnames = self.dic['flankseq_colnames']
            if 'flankstrand_vals' not in self.dic:
                thesefstrand = [None for f in theseflanks]
            else:
                thesefstrand = self.dic['flankstrand_vals']
        else: # single cols
            theseflanks.append(self.dic['flankseq'])
            thesecolnames.append(self.dic['flankseq_colname'])
            if 'flankstrand_val' not in self.dic:
                thesefstrand.append(None)
            else:
                thesefstrand.append(self.dic['flankstrand_val'])
        for ind,thisflank in enumerate(theseflanks):
            toadd = True
            for dbflank,dbflankds in indb:
                matchtype = self.flankmatch(thisflank,dbflank)
                if matchtype == 1: # match found, needs to be distinct ds to be added to match_count table
                    #self.countplus(dbflank,"flank","flank_seq")
                    if self.datasource != dbflankds:
                        self.addmatch(dbflank,"flank")
                    toadd = False
                    break
                if matchtype == 2: # swap in thisflank
                    self.squeezeflank(dbflank) # precedes insertflank to delete from flank table and add to match_count table
                    break
            if toadd:
                self.insertflank(thisflank,thesecolnames[ind],thesefstrand[ind],multiple) 

    def log_probe(self):
        self.curs.execute("SELECT probe_seq,datasource FROM probes WHERE id = %s",(self.main_id,))    
        indb = self.curs.fetchall() #list of tuples
        theseprobes = [] #probseq_seqs
        thesecolnames = [] #probseq_colnames
        thesepstrand = [] # no examples yet, from chipreader.probstrand_cols
        multiple = False
        if 'probseq_seqs' in self.dic:
            multiple = True
            theseprobes = self.dic['probseq_seqs']
            thesecolnames = self.dic['probseq_colnames']
            if 'probestrand_vals' not in self.dic:
                thesepstrand = [None for p in theseprobes]
            else:
                thesepstrand = self.dic['probestrand_vals']
        #TODO elif single cols, but not 'else' for when no probe sequences at all
        for ind,thisprobe in enumerate(theseprobes):
            toadd = True
            for dbprobe,dbprobeds in indb:
                matchtype = self.flankmatch(thisprobe,dbprobe,probe=True)
                if matchtype == 1:
                    if self.datasource != dbprobeds:
                        self.addmatch(dbprobe,"probes")
                    toadd = False
                    break
                if matchtype == 2:
                    self.squeezeflank(dbprobe,probe=True)
                    break
            if toadd:
                self.insertflank(thisprobe,thesecolnames[ind],thesepstrand[ind],multiple,probe=True)

    def log_coord(self):
        self.curs.execute("SELECT pos FROM positions WHERE id = %s AND build = %s",(self.main_id,self.build))
        chrm = self.dic['chr'] #TODO: support alternative labels for X, Y. MT
        pos = self.dic['pos']
        indb = self.curs.fetchall()
        indb = [st[0] for st in indb] 
        assert isinstance(pos,int)
        posadd = True
        for dbpos in indb:
            assert isinstance(dbpos,int)
            if dbpos == pos:
                posadd = False
        if posadd:
            vals = (self.main_id,chrm,pos,self.build,self.datasource)
            self.curs.execute("INSERT INTO positions (id,chr,pos,build,datasource) VALUES (%s,%s,%s,%s,%s)",vals)
