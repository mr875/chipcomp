from queryfile import NormFile
from connect import DBConnect
import sys

'''
chipcursor.execute(query2)
for row in chipcursor.fetchall():
    print(row)
'''

def addnewid(cols,curs):
    pass

def main(argv):
    fname = 'exsnp/nors_j_dbsnp.txt'
    if argv:
        fname = argv[0]
    else:
        print("no file provided, instead using default file %s to act as 3 column join file (chr:pos old_id rsid)" % (fname))
    joinfile = NormFile(fname)
    linecount = joinfile.row_count
    db = DBConnect("chip_comp")
    curs = db.getCursor()
    for num,cols in enumerate(joinfile.readls(" ")):
        addnewid(cols,curs)
        if num == 10:
            break
    db.close()

if __name__ == '__main__':
    main(sys.argv[1:])

