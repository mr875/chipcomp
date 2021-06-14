import logging
import resolvef
import re

class VariantI:

    def __init__(self,curs,row_dic,ds,build,report_mode = False):
        self.dic = row_dic
        self.secondary_id = None
        self.main_id = None
        self.datasource = ds
        self.build = build
        self.report_mode = report_mode
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
        if self.report_mode:
            logging.info('to add new main id %s into consensus, ds %s. and secondary id if present (%s) to alt ids' % (prim_id,dsource,sec))
        else:
            self.curs.execute("INSERT INTO consensus (id,uid_datasource) VALUES (%s, %s)",(prim_id,dsource))
            if sec:
                self.curs.execute("INSERT INTO alt_ids (id, alt_id, datasource) VALUES (%s,%s,%s)",(prim_id,sec,dsource))

    def add_altid_old(self,prim_id,sec_id,new_ds): # deprecated
        self.curs.execute("SELECT EXISTS(SELECT * FROM alt_ids WHERE id = %s AND alt_id = %s AND datasource = %s)",(prim_id,sec_id,new_ds))
        if not self.curs.fetchone()[0]:
            if self.report_mode:
                logging.info('alternative id required. %s is alt to %s, ds %s' % (sec_id,prim_id,new_ds))
            else:
                self.curs.execute("INSERT INTO alt_ids (id, alt_id, datasource) VALUES (%s, %s, %s)",(prim_id,sec_id,new_ds))
    
    def add_altid(self,prim_id,sec_id,new_ds): # SELECT EXISTS appears to be faster than pulling out values
        self.curs.execute("SELECT EXISTS(SELECT 1 FROM alt_ids WHERE id = %s AND alt_id = %s AND datasource = %s)",(prim_id,sec_id,new_ds))
        if not self.curs.fetchone()[0]:
            self.curs.execute("SELECT EXISTS(SELECT 1 from alt_ids WHERE alt_id = %s)",(sec_id,))
            if self.curs.fetchone()[0]:
                logging.info('alternative id %s for %s to be added but it is already an alternative to a different main id. Not adding. Can possibly be resolved by merging the main ids' % (sec_id,prim_id))
            else:
                if self.report_mode:
                    logging.info('alternative id required. %s is alt to %s, ds %s' % (sec_id,prim_id,new_ds))
                else:
                    self.curs.execute("INSERT INTO alt_ids (id, alt_id, datasource) VALUES (%s, %s, %s)",(prim_id,sec_id,new_ds))
    
    def snpid_swapin(self,uid_to_swapout,db_snp,old_ds,new_ds,this_altid=None):
        #swap dbsnp id into consensus table, put initial id into alt_ids
        if self.report_mode:
            logging.info('dbsnp id %s to be swapped in, ds %s and old main id %s to go as alt id, ds %s' % (db_snp,new_ds,uid_to_swapout,old_ds))
        else:
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
        if self.report_mode:
            logging.info('to register (or ignore) uid %s as being present in ds %s' % (uid,datasource))
        else:
            self.curs.execute("INSERT IGNORE INTO snp_present (id,datasource) VALUES (%s,%s)",(uid,datasource))

    def log_dbsnpid(self):
        self.main_id = self.dbsnpid
        snp_exist = self.id_exists(self.dbsnpid)
        snp_alt = None
        if not snp_exist:
            snp_alt = self.altid_exists(self.dbsnpid)
            if snp_alt: # already logged
                self.main_id = snp_alt[0]
        if snp_exist or snp_alt: 
            if self.secondary_id: #snpid found but if sec id is new, add to alt_ids
                self.add_altid(self.main_id,self.secondary_id,new_ds=self.datasource)
            return
        if self.secondary_id: #snpid not in consensus but if the uid contains a non dbsnp id
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
                    old_ds = self.id_exists(uid_to_swapout)[0]
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

    @classmethod
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

    @classmethod
    def rev(self,seq): # repeated: should reuse already-written methods but original design does not facilitate this well enough 
        switchdic = {"A":"T","C":"G","G":"C","T":"A","[":"]","]":"[","/":"/","-":"-"}
        revseq = ''.join(switchdic[n] for n in seq[::-1])
        return revseq

    @classmethod
    def flankmatch_allow_indel(self,f1,f2,trim=1): # mentions indel but this is really about comparing left and right separately to make it eaiser to get a match, if necessary by trimming one of the sides.
        left_fit = False
        right_fit = False
        f1_nofit = ''
        f2_nofit = ''
        f1_left = f1.split('[')[0]
        f1_right = f1.split(']')[1]
        f2_left = f2.split('[')[0]
        f2_right = f2.split(']')[1]
        if f1_left in f2_left or f2_left in f1_left:
            left_fit = True
        else:
            f1_nofit = f1_left
            f2_nofit = f2_left
        if f1_right in f2_right or f2_right in f2_right:
            right_fit = True
            if left_fit:
                return 1
        else:
            f1_nofit = f1_right
            f2_nofit = f2_right
            if not left_fit:
                return 0 # neither left or right sides fit each other. trimming is only attempted on non matching side but at least 1 side should match without trimming
        f1inf2 = self.trim_and_match(f1_nofit,f2_nofit,trim)
        f2inf1 = self.trim_and_match(f2_nofit,f1_nofit,trim)
        if f1inf2 or f2inf1:
            return 1
        else:
            return 0

    @classmethod
    def trim_and_match(self,seq1,seq2,trim):
        fits = False
        for i in range(trim):
            t = i + 1
            if seq1[t:] in seq2:
                fits = True
                break
            if seq1[:-t] in seq2:
                fits = True
                break
        return fits

    @classmethod
    def indel_correction(self,flank):
        if "[-/" in flank: #insertions of more than 1 bp change the sequence on the right hand side of ']' so shift all but the 1st one: GCG[-/CA]CAGA becomes GCG[-/C]ACAGA. This is just for matching purposes. the edit is not maintained anywhere
            dpart = re.findall(r"\[-/([A-Za-z]+)",flank)[0]
            shiftpart = dpart[1:]
            keeppart = dpart[0]
            left = flank.split('[')[0]
            right = flank.split(']')[1]
            newflank = left + '[-/' + keeppart + ']' + shiftpart + right
            flank = newflank
        return flank

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
        select = "SELECT datasource,chosen "
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
        oldchos = rowlist[0][1]
        if not oldchos:
            if self.report_mode:
                logging.info('to remove flank (or probe) seq %s from id %s most likely because it will be replaced with a longer version' % (dbflank,self.main_id))
            else:
                self.curs.execute(delete+row,where)
            return 1
        else:
            logging.info('prevented from removing flank (or probe) seq %s from id %s (it was to be replaced with a longer version) because this sequence has been validated/chosen at a previous step' % (dbflank,self.main_id))
            return 0
    
    def insertflank(self,thisflank,colname,fstrand,multiple,probe=False):
        q = "INSERT INTO flank (id,colname,datasource,flank_seq,flank_strand,multiple) VALUES (%s, %s, %s, %s, %s, %s)"
        if probe:
            q = "INSERT INTO probes (id,colname,datasource,probe_seq,probe_strand,multiple) VALUES (%s, %s, %s, %s, %s, %s)"
        vals = (self.main_id, colname, self.datasource,thisflank,fstrand,multiple)
        if self.report_mode:
            logging.info('flank (or probe) sequence to be inserted: %s for id %s, ds %s' % (thisflank,self.main_id,self.datasource))
        else:
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
            xhelp = False
            for dbflank,dbflankds in indb:
                matchtype = self.flankmatch(thisflank,dbflank)
                if not matchtype:
                    matchtype = self.flankmatch(self.rev(thisflank),dbflank)
                if not matchtype:
                    xhelp = True
                    trim = 1 # how much to try trimming off one side to help match (may help with some indels)
                    matchtype = self.flankmatch_allow_indel(thisflank,dbflank,trim)
                    if not matchtype:
                        matchtype = self.flankmatch_allow_indel(self.rev(thisflank),dbflank,trim)
                if matchtype == 1: # match found, needs to be distinct ds to be added to match_count table
                    if xhelp:
                        logging.info('less stringent matching of file flank %s with db flank %s used for %s' % (thisflank,dbflank,self.main_id))
                    toadd = False
                    break
                if matchtype == 2: # swap in thisflank
                    success = self.squeezeflank(dbflank) # precedes insertflank to delete from flank table and add to match_count table
                    if not success:
                        toadd = False
                    break
            if toadd:
                self.insertflank(thisflank,thesecolnames[ind],thesefstrand[ind],multiple) 

    def log_probe(self):
        self.curs.execute("SELECT probe_seq,datasource FROM probes WHERE id = %s",(self.main_id,))    
        indb = self.curs.fetchall() #list of tuples
        theseprobes = [] #probseq_seqs
        thesecolnames = [] #probseq_colnames
        thesepstrand = [] # no examples yet, from chipreader.probstrand_cols or chipreader.col_probe_strand if single
        multiple = False
        if 'probseq_seqs' in self.dic:
            multiple = True
            theseprobes = self.dic['probseq_seqs']
            thesecolnames = self.dic['probseq_colnames']
            if 'probestrand_vals' not in self.dic:
                thesepstrand = [None for p in theseprobes]
            else:
                thesepstrand = self.dic['probestrand_vals']
        elif 'probseq' in self.dic: # single col
            theseprobes.append(self.dic['probseq'])
            thesecolnames.append(self.dic['probseq_colname'])
            if 'probestrand_val' not in self.dic:
                thesepstrand.append(None) 
            else:
                thesepstrand.append(self.dic['probestrand_val'])
        for ind,thisprobe in enumerate(theseprobes):
            toadd = True
            for dbprobe,dbprobeds in indb:
                matchtype = self.flankmatch(thisprobe,dbprobe,probe=True)
                if matchtype == 1:
                    #if self.datasource != dbprobeds:
                    #    self.addmatch(dbprobe,"probes")
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
            if self.report_mode:
                logging.info('will add (new) position %s:%s to build %s for id %s ds %s' % (chrm,pos,self.build,self.main_id,self.datasource))
            else:
                vals = (self.main_id,chrm,pos,self.build,self.datasource)
                self.curs.execute("INSERT INTO positions (id,chr,pos,build,datasource) VALUES (%s,%s,%s,%s,%s)",vals)

class NotMerged(Exception):
    pass

class VariantM(VariantI):

    def __init__(self,curs,main_id,pos,build,ds,chrm):
        self.main_id = main_id
        self.datasource = ds
        self.build = build
        self.curs = curs
        self.pos = pos
        self.chr = chrm

    def snpid_swapin(self,alt_id,alt_ds):
        opp_build = '38'
        q = "SELECT pos FROM positions WHERE build = %s AND id = %s AND pos <> 0"
        if self.build == '38':
            opp_build = '37'
        self.curs.execute(q,(opp_build,alt_id))
        opp_alt = self.curs.fetchall()
        self.curs.execute(q,(opp_build,self.main_id))
        opp_main = self.curs.fetchall()
        #going to merge multiple ids that have the same position as self.main_id, but check opposite build first
        if len(opp_alt):
            if len(opp_alt) > 1: #alt id shouldn't be appearing more than once. raise error for logging
                raise NotMerged("swapping alt id %s for main id %s in build %s but detected multiple positions for alt id in opposite build %s" % (alt_id,self.main_id,self.build,opp_build)) 
            if len(opp_main): # both main id and alt id have entries in opposite build
                if len(opp_main) > 1: # main id shouldn't be appearing more than once. raise error for logging
                    raise NotMerged("swapping alt id %s for main id %s in build %s but detected multiple positions for main id in opposite build %s" % (alt_id,self.main_id,self.build,opp_build))
                opp_alt_pos = opp_alt[0][0]
                opp_main_pos = opp_main[0][0]
                if opp_alt_pos == opp_main_pos:# positions match so we can remove alt id from opposite build because main id exists
                    q = "DELETE FROM positions WHERE build = %s AND id = %s AND pos = %s"
                    vals = (opp_build,alt_id,opp_alt_pos)
                    self.curs.execute(q,vals)
                else:# main id and alt id already in opp build but with different positions logged so can't do a clean swap for this alt_id
                    raise NotMerged("swapping alt id %s for main_id %s in build %s but detected both alt and main ids already in opposite build %s but do not have the same positions (%s vs %s)" % (alt_id,self.main_id,self.build,opp_build,opp_alt_pos,opp_main_pos))
            else: # alt id in opposite build but main id not, so switch the main id in 
                q = "UPDATE positions SET id = %s WHERE build = %s AND id = %s AND pos = %s"
                vals = (self.main_id,opp_build,alt_id,opp_alt[0][0]) 
                self.curs.execute(q,vals) 
        #else: # alt id not in opposite build, no change necesary 
        self.curs.execute("DELETE FROM positions WHERE id = %s AND build = %s",(alt_id,self.build))
        self.curs.execute("UPDATE IGNORE alt_ids SET id = %s WHERE id = %s",(self.main_id,alt_id))
        self.curs.execute("DELETE FROM alt_ids WHERE id = %s",(alt_id,))
        self.curs.execute("INSERT IGNORE INTO alt_ids (id, alt_id, datasource) VALUES (%s, %s, %s)",(self.main_id,alt_id,alt_ds))
        self.curs.execute("UPDATE IGNORE flank SET id = %s WHERE id = %s",(self.main_id,alt_id))
        self.curs.execute("DELETE FROM flank WHERE id = %s",(alt_id,))
        self.curs.execute("UPDATE IGNORE probes SET id = %s where id = %s",(self.main_id,alt_id))
        self.curs.execute("DELETE FROM probes WHERE id = %s",(alt_id,)) 
        self.curs.execute("UPDATE IGNORE snp_present SET id = %s WHERE id = %s",(self.main_id,alt_id)) 
        self.curs.execute("DELETE FROM snp_present WHERE id = %s",(alt_id,)) # if alt_id not updated because main_id--datasource already exists (ignored) then remove it
        self.curs.execute("UPDATE IGNORE match_count SET id = %s where id = %s",(self.main_id,alt_id))
        self.curs.execute("DELETE FROM match_count WHERE id = %s",(alt_id,))
        self.curs.execute("DELETE FROM consensus WHERE id = %s",(alt_id,))
