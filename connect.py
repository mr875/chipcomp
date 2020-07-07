import mysql.connector
import configparser
""" Example Usage
chip=DBConnect("chip_comp")
query2 = ("show tables")
chipcursor=chip.getCursor()
chipcursor.execute(query2)
for row in chipcursor.fetchall():
    print(row)
chip.close()
"""
class DBConnect:

    def __init__(self,db="br",usr=None,pw=None,hst="localhost"):
        if not usr and not pw:
            self.loadConf()
        else:
            self.user=usr
            self.pw=pw
        self.dbs = mysql.connector.connect(user=self.user, password=self.pw, host=hst, database=db)
        #self.cursor = self.dbs.cursor()
        self.cursors = []

    def loadConf(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.user=config['user']['name']
        self.pw=config['user']['pw']

    def getCursor(self):
        new_curs = self.dbs.cursor()
        self.cursors.append(new_curs)
        return new_curs

    def resetCursors(self):
        for c in self.cursors:
            c.close()
        self.cursors=[]

    def commit(self):
        self.dbs.commit()

    def close(self):
        self.resetCursors() 
        self.dbs.close()

