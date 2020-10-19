import datetime
import mysql.connector
import config as cfg
import sqlalchemy as sqal

cnx = mysql.connector.connect(user=cfg.mysql['user'],
                              password= cfg.mysql['pwd'],
                              database=cfg.mysql['db'])
mycursor = cnx.cursor()

mycursor.execute("SELECT COUNT(hit_id) FROM qreuz_table")

myresult = mycursor.fetchall()

for x in myresult:
  print(x)

mycursor.close()
cnx.close()