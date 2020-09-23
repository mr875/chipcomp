from queryfile import NormFile
import requests
import urllib.request
import xml.etree.ElementTree as ET

class XMLParseError(Exception):
    pass

def getflanks(rsid):
    url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=snp&id=%s&rettype=fasta&retmode=xml"
    url = url % (rsid)
    req = requests.get(url)
    root = ET.fromstring(req.content)
    namesp = root.tag.split("}")[0] + "}" # eg: {https://www.ncbi.nlm.nih.gov/SNP/docsum}DocumentSummary
    tofind = namesp + 'DocumentSummary'
    main = root.find(tofind)
    if not main:
        raise XMLParseError("trouble getting %s Element from root for rsid %s" % (tofind,rsid))        
    tofind = namesp + 'CHRPOS'
    poselm = main.find(tofind)
    if poselm:
        pos = poselm.text
    else:
        raise XMLParseError("trouble finding %s Element for rsid %s" % (tofind,rsid))
    tofind = namesp + 'ACC'
    accelm = main.find(tofind)
    if accelm:
        acc = accelm.text
    else:
        raise XMLParseError("trouble finding %s Element for rsid %s" % (tofind,rsid))
    allele = main.find(namesp + 'ALLELE').text
    genename = main.find(namesp + 'GENES/' + namesp + 'GENE_E/' + namesp + 'NAME').text
    print(pos,acc,allele,genename)

def main():
    print("start") 
    rsids = NormFile('rsids.txt')
    for line in rsids.readls():
        getflanks(line[0])

if __name__ == '__main__':
    main()
