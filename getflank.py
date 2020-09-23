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
        print("a problem occurred when trying to fetch (or load):",url)
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
        raise XMLParseError(message % (tofind,rsid))
    tofind = namesp + 'GENES/' + namesp + 'GENE_E/' + namesp + 'NAME'
    genename_elm = main.find(tofind)
    if genename_elm is not None:
        genename = genename_elm.text
    else:
        raise XMLParseError(message % (tofind,rsid))
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
    print("start") 
    rsids = NormFile('rsids.txt')
    for line in rsids.readls():
        chrm,pos,acc,allele,genename = getdetails(line[0])
        left,right = getflank(pos,acc)
        print("%s: %s[]%s" % (line[0],left,right))

if __name__ == '__main__':
    main()
