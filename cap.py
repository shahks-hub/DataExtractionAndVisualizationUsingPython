import sqlite3
import ssl
import urllib.request, urllib.error
import json

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE



conn = sqlite3.connect('smallbusiness.sqlite')
cur = conn.cursor()


cur.execute('''
CREATE TABLE IF NOT EXISTS Company (org TEXT, count INTEGER)''')
cur.execute('''
CREATE TABLE IF NOT EXISTS Address (addr TEXT, count INTEGER)''')
cur.execute('''
CREATE TABLE IF NOT EXISTS Types (type TEXT, count INTGER)''')

dataurl = input('Enter file name: ')
if len(dataurl) < 1:
    dataurl= 'https://data.hartford.gov/api/views/aazy-e39h/rows.json?'
print('Retrieving', dataurl)
uh = urllib.request.urlopen(dataurl, context=ctx)
data = uh.read().decode()
print('Retrieved', len(data), 'characters')
info = json.loads(data)
count=0

#print(data)
#type of service
#extracting names of the small businesses and their count
area= info['meta']['view']['name']
city= info['meta']['view']['attribution']
print("Region data obtained for",area)
print(city)

for names in info['data']:
    name = names[8]
    type = names[11]
    addr =names[10]
    count = count + 1

# counts
    cur.execute('Select count FROM Company WHERE org=?',(name,))
    row=cur.fetchone()
    if row is None:
        cur.execute("INSERT INTO Company(org,count) VALUES(?,1)",(name,))

    else:
       cur.execute('UPDATE Company SET count = count + 1 WHERE org=?',(name,))

# Types

    cur.execute('Select count FROM Types WHERE type=?', (type,))

    row=cur.fetchone()
    if row is None:
        cur.execute("INSERT INTO Types(type,count) VALUES(?,1)",(type,))

    else:
       cur.execute('UPDATE Types SET count =count+1 WHERE type=?',(type,))

#addr

    cur.execute('Select count FROM Address WHERE addr=?', (addr,))
    row=cur.fetchone()
    if row is None:
        cur.execute("INSERT INTO Address(addr,count) VALUES(?,1)",(addr,))
    else:
        cur.execute("UPDATE Address SET count=count+1 WHERE addr=?",(addr,))







print('Number of small businesses in this dataset:',count)
conn.commit()


sqlstr = 'SELECT org, count FROM Company ORDER BY count DESC LIMIT 10'

for row in cur.execute(sqlstr):
    print(str(row[0]), row[1])


sqlstr2 = 'SELECT type, count FROM Types ORDER BY count DESC LIMIT 10'

for row in cur.execute(sqlstr2):
    print(str(row[0]), row[1])


sqlstr3 = 'SELECT addr, count FROM Address ORDER BY count DESC LIMIT 10'

for row in cur.execute(sqlstr3):
    print(str(row[0]), row[1])


cur.close()





