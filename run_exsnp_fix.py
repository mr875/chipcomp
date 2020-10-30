import sys
from queryfile import NormFile
from connect import DBConnect

def buildrsmerge(rsid,refpos,altid):
    print("entered buildrsmerge")

def xyrsmerge(rsid,refpos,altid):
    print("entered xyrsmerge")

def main(argv):
    
    if len(argv) < 1:
        print("provide file with build 37 merges into build 38 rs ids (eg: exsnp/buildispar.txt) OR file with X/XY rsid merges (eg: exsnp/xydispar.txt)")
        return
    else:
        infile = NormFile(argv[0])
    conn = DBConnect("chip_comp")
    curs = conn.getCursor()
    for rsid,refpos,altid in infile.readls(dlim=" "):
        if refpos.split(':')[0].isnumeric():
            buildrsmerge(rsid,refpos,altid)
        else:
            xyrsmerge(rsid,refpos,altid)
    conn.close()

if __name__ == '__main__':
    main(sys.argv[1:])

