import mysql.connector

class DBConnect:

    def __init__(self,usr,pw,db="br",hst="localhost"):
        self.dbs = mysql.connector.connect(user=usr, password=pw, host=hst, database=db)
        self.cursor = self.dbs.cursor()

    def getCursor(self):
        return self.cursor

    def close(self):
        self.cursor.close()
        self.dbs.close()

