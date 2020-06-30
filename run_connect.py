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
br.close()
chip.close()
