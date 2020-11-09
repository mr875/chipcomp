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
        self.sock = None
        if not usr and not pw:
            self.loadConf()
        else:
            self.user=usr
            self.pw=pw
        if self.sock: # a different way to connect if running mysqq server locally on HPC
           self.dbs = mysql.connector.connect(unix_socket=self.sock, database=db, user=self.user, password=self.pw) 
        else:
            self.dbs = mysql.connector.connect(user=self.user, password=self.pw, host=hst, database=db)
        self.cursors = []

    def loadConf(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.user=config['user']['name']
        self.pw=config['user']['pw']
        if 'socket' in config['user']:
            self.sock = config['user']['socket']


    def getCursor(self,dic=False):
        if dic:
            new_curs = self.dbs.cursor(dictionary=True)
        else:
            new_curs = self.dbs.cursor()

        self.cursors.append(new_curs)
        return new_curs

    def resetCursors(self):
        for c in self.cursors:
            c.close()
        self.cursors=[]

    def commit(self):
        self.dbs.commit()

    def rollback(self):
        self.dbs.rollback()

    def close(self):
        self.resetCursors() 
        self.dbs.close()

