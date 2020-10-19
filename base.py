import config as cfg
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


# creating connection engine
engine = db.create_engine("mysql://"+cfg.mysql['user']+
                          ":"+cfg.mysql['pwd']+
                          "@"+cfg.mysql['host']+
                          "/"+cfg.mysql['db'],
                          echo = False)
connection = engine.connect()
metadata = db.MetaData()
Session = sessionmaker(bind=engine)
Base = declarative_base()

# assigning table
qreuz_table = db.Table('qreuz_table', metadata, autoload=True, autoload_with=engine)



