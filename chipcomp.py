from connect import DBConnect

br=DBConnect("Marc","marc99")
query = ("select * from coreexome_frq limit 1")
brcursor = br.getCursor()
brcursor.execute(query)
for row in brcursor.fetchall():
    print(row)
chip=DBConnect("Marc","marc99","chip_comp")
br.close()
chip.close()
