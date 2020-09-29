import sys
import time
import datetime
import logging
from queryfile import NormFile
import requests
import urllib.request
import xml.etree.ElementTree as ET

class XMLParseError(Exception):
    pass

def getdetails(rsid):
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=snp&id=%s&rettype=fasta&retmode=xml"
    url = url % (rsid)
    try:
        req = requests.get(url)
        root = ET.fromstring(req.content)
    except Exception:
        logging.error("a problem occurred when trying to fetch (or load):",url)
    message = "trouble finding %s Element for rsid %s:\n" + url
    namesp = root.tag.split("}")[0] + "}" # eg: {https://www.ncbi.nlm.nih.gov/SNP/docsum}DocumentSummary
    tofind = namesp + 'DocumentSummary'
    main = root.find(tofind)
    if not main:
        raise XMLParseError(message % (tofind,rsid))        
    tofind = namesp + 'CHRPOS'
    poselm = main.find(tofind)
    if poselm is not None:
        pos = poselm.text
    else:
        raise XMLParseError(message % (tofind,rsid))
    tofind = namesp + 'ACC'
    accelm = main.find(tofind)
    if accelm is not None:
        acc = accelm.text
    else:
        raise XMLParseError(message % (tofind,rsid))
    tofind = namesp + 'ALLELE'
    allele_elm = main.find(tofind)
    if allele_elm is not None:
        allele = allele_elm.text
    else:
        logging.info("%s: can't find %s\nbut not needed" % (rsid,tofind))
        #raise XMLParseError(message % (tofind,rsid))
    tofind = namesp + 'GENES/' + namesp + 'GENE_E/' + namesp + 'NAME'
    genename_elm = main.find(tofind)
    if genename_elm is not None:
        genename = genename_elm.text
    else:
        logging.info("%s: can't find %s\nbut not needed" % (rsid,tofind))
        #raise XMLParseError(message % (tofind,rsid))
    chrm = pos.split(':')[0]
    pos = pos.split(':')[1]
    return (chrm,pos,acc,allele,genename)

def getflank(pos,acc):
    pos = int(pos)
    lrlength = 50
    leftstart = pos - lrlength
    leftend = pos - 1
    rightstart = pos + 1
    rightend = pos + lrlength
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=nuccore&id=%s&seq_start=%s&seq_stop=%s&rettype=fasta&retmode=text"
    lefturl = url % (acc,leftstart,leftend)
    righturl = url % (acc,rightstart,rightend)
    req = requests.get(lefturl)
    left = req.text.rstrip().split('\n')[1]
    req = requests.get(righturl)
    right = req.text.rstrip().split('\n')[1]
    return left,right 

def main():
    rsids = NormFile('exflank/rsids.txt')
    batch = 500
    rc = rsids.row_count
    logfile = datetime.datetime.now().strftime("gfl_%a_%d%b_%I%p.log")
    logging.basicConfig(filename=logfile, level=logging.INFO)
    fvper = int(0.05 * batch)
    logline = fvper
    start = time.time()
    startid = ""
    strt_mess = 'from beginning of file'
    if startid:
        strt_mess = "from line containing '" + startid + "'"
    count = 0
    fail_count = 0;
    max_fail = 10
    logging.info('file %s has %s lines. Doing a batch of %s lines %s.' % (rsids.bfile,rc,batch,strt_mess))
    exfl = open('exflank/external_flanks.txt','a+')
    cont = True
    endof = True
    for line in rsids.readls():
        if line[0] == startid:
            cont = False
        if cont and startid:
            continue
        count += 1
        if count > batch:
            endof = False
            now = int(time.time() - start)
            logging.info('finished batch of %s. Next line: %s. Took %s seconds' % (batch,line[0],now))
            break
        if logline == count:
            now = int(time.time() - start)
            logging.info("approximately %.2f%% of batch done. %s seconds. line %s" % ((count/batch*100),now,line[0]))
            logline += fvper
        try:
            chrm,pos,acc,allele,genename = getdetails(line[0])
            left,right = getflank(pos,acc)
            exfl.write("%s\t%s[]%s\n" % (line[0],left,right))
        except Exception as e:
            fail_count+=1
            statement = "error (%s) encountered at line/rs %s\n%s\n%s" % (fail_count,line[0],str(sys.exc_info()[0]),str(e))
            logging.error(statement)
            if fail_count >= max_fail:
                exfl.close()
                statement = "maximum error count reached, leaving process"
                logging.error(statement)
                raise 
    exfl.close()
    if endof:
        now = int(time.time() - start)
        logging.info('reached end of file %s after %s seconds' % (rsids.bfile,now))

if __name__ == '__main__':
    main()
