import config as cfg
import sqlalchemy as db
import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

'''
logging.basicConfig()
logging.getLogger('db').setLevel(logging.ERROR)
'''

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
'''
# defining queries task 1
query = db.select([db.func.count(qreuz_table.columns.hit_id)])
query_2 = db.select([db.func.count(qreuz_table.columns.user_agent.distinct())])
query_3= db.select([qreuz_table.columns.session_id,
    db.func.count(qreuz_table.columns.session_id).label('qty')]).group_by(qreuz_table.columns.session_id
    ).order_by(db.desc('qty'))

# executing queries
result_1 = connection.execute(query).scalar()
result_2 = connection.execute(query_2).scalar()
result_3 = connection.execute(query_3)
result_3 = list(result_3.fetchall()[0])[1]

# printing results
print('Number of hits: ' + str(result_1))
print('Number of unique entries for user_agent: '+ str(result_2))
print('Most occurence of session_id: '+ str(result_3))
'''


