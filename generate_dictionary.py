# -*- coding: utf-8 -*-
#rachmat_sn

import sqlite3
import json

conn = sqlite3.connect('scrapeolx.db')
c = conn.cursor()

data = {} #dictionary
brand = ['honda', 'yamaha', 'suzuki', 'kawasaki']

#10 kota/kabupaten dengan penjual merek honda, yahama, suzuki, kawasaki terbanyak
query = c.execute("SELECT city FROM motorBekas WHERE (brand LIKE '%"+brand[0]+"%' or brand LIKE '%"+brand[1]+"%' or brand LIKE '%"+brand[2]+"%' or brand LIKE '%"+brand[3]+"%') GROUP BY city ORDER BY COUNT(city) DESC LIMIT 10")
ftch = c.fetchall()
kab_kota = []
for i in range (len(ftch)):
    kab_kota.append(ftch[i][0])
data['labels'] = kab_kota

#honda, yamaha, suzuki, kawasaki di kota-kota
for i in range(len(kab_kota)):
    result = []
    for j in range(len(brand)):
        query = c.execute("SELECT count(brand) FROM motorBekas WHERE brand LIKE '%"+brand[j]+"%' and city LIKE '%"+kab_kota[i]+"%'")
        ftch = c.fetchall()
        result.append(int(ftch[0][0]))
    data["k"+str(i+1)] = result

print(data)

#simpan file
with open('visualisasi_dictionary.txt', 'w') as fp:
    json.dump(data, fp)
