from connect import DBConnect

br=DBConnect()
query = ("select * from coreexome_frq limit 1")
brcursor = br.getCursor()
brcursor.execute(query)
for row in brcursor.fetchall():
    print(row)
chip=DBConnect("chip_comp")
query2 = ("show tables")
chipcursor=chip.getCursor()
chipcursor.execute(query2)
for row in chipcursor.fetchall():
    print(row)
query3 = ('select * from dil_frq limit 1')
brcursor.execute(query3)
for row in brcursor.fetchall():
    print(row)
br.close()
chip.close()
#todo: cursor should probably be closed after use and can then open a new one. should move this behaviour to DBConnect
#https://stackoverflow.com/questions/5504340/python-mysqldb-connection-close-vs-cursor-close
