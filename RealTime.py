import urllib
import requests
import psycopg2
from bs4 import BeautifulSoup

dbname='dbname'
user='user'
host='127.0.0.1'
password='password'

url = "http://oasis.caiso.com/oasisapi/prc_hub_lmp/PRC_HUB_LMP.html"
response = requests.get(url)
soup = BeautifulSoup(response.content, "html.parser")
CAISO_Time = soup.find_all('h1')[1]
CAISO_Time = CAISO_Time.text.strip()
CAISODate = CAISO_Time.split("\n\t\t")[1]
CAISOHour = CAISO_Time.split("\n\t\t")[3].strip()
CAISOInterval = CAISO_Time.split("\n\t\t")[6].strip()

NP15_Box = soup.find('td',text='NP15').parent
NP15_Price = NP15_Box.text.strip()
NP15lmp = NP15_Price.split("$\n\t\t\t\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t\t\t")[1]
NP15energy = NP15_Price.split("$\n\t\t\t\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t\t\t")[2]
NP15congestion = NP15_Price.split("$\n\t\t\t\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t\t\t")[3]
NP15losses = NP15_Price.split("$\n\t\t\t\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t\t\t")[4]

SP15_Box = soup.find('td',text='SP15').parent
SP15_Price = SP15_Box.text.strip()
SP15lmp = SP15_Price.split("$\n\t\t\t\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t\t\t")[1]
SP15energy = SP15_Price.split("$\n\t\t\t\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t\t\t")[2]
SP15congestion = SP15_Price.split("$\n\t\t\t\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t\t\t")[3]
SP15losses = SP15_Price.split("$\n\t\t\t\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t\t\t")[4]

ZP26_Box = soup.find('td',text='ZP26').parent
ZP26_Price = ZP26_Box.text.strip()
ZP26lmp = ZP26_Price.split("$\n\t\t\t\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t\t\t")[1]
ZP26energy = ZP26_Price.split("$\n\t\t\t\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t\t\t")[2]
ZP26congestion = ZP26_Price.split("$\n\t\t\t\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t\t\t")[3]
ZP26losses = ZP26_Price.split("$\n\t\t\t\t\t\t\t\t\t\t\n\t\t\t\t\t\t\t\t\t\t")[4]

#Write results to database
sql="""INSERT INTO caiso (pnode, createdatetime, lmp, energy, congestion, losses, date, hour, interval, market, updatedatetime)
Values ('NP15', now(), %s,%s,%s,%s,%s,%s,%s, 'RT', now()),
       ('SP15', now(), %s,%s,%s,%s,%s,%s,%s, 'RT', now()),
       ('ZP26', now(), %s,%s,%s,%s,%s,%s,%s, 'RT', now());""";


connection=f"dbname={dbname} user={user} host={host} password={password}"
conn=psycopg2.connect(connection);
cur=conn.cursor();
cur.execute(sql,(NP15lmp,NP15energy,NP15congestion,NP15losses,CAISODate,CAISOHour,CAISOInterval,
     SP15lmp,SP15energy,SP15congestion,SP15losses,CAISODate,CAISOHour,CAISOInterval,
     ZP26lmp,ZP26energy,ZP26congestion,ZP26losses,CAISODate,CAISOHour,CAISOInterval));
conn.commit();
cur.close()

