import mysql.connector
import configparser

class DBConnect:

    def __init__(self,db="br",usr=None,pw=None,hst="localhost"):
        if not usr and not pw:
            self.loadConf()
        else:
            self.user=usr
            self.pw=pw
        self.dbs = mysql.connector.connect(user=self.user, password=self.pw, host=hst, database=db)
        self.cursor = self.dbs.cursor()

    def loadConf(self):
        config = configparser.ConfigParser()
        config.read('config.ini')
        self.user=config['user']['name']
        self.pw=config['user']['pw']

    def getCursor(self):
        return self.cursor

    def close(self):
        self.cursor.close()
        self.dbs.close()

