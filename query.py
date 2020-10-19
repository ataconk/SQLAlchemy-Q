from base import Base,engine,Session,qreuz_table
import sqlalchemy as db
from sqlalchemy import String
import json

session = Session()
connection = engine.connect()


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
# print('Number of hits: ' + str(result_1))
# print('Number of unique entries for user_agent: '+ str(result_2))
# print('Most occurence of session_id: '+ str(result_3))


entries = session.query(qreuz_table.columns.ad_ids).all()
counter = 0
for entry in entries:
    entry = json.loads(str(entry)[2:-3])['utm_campaign']
    if entry == 'test003':
        counter = counter+1
# print('Number of "test003": '+ str(counter))

my_list = []
all_entries = session.query(qreuz_table).all()
for entry in all_entries:
    utm = json.loads(entry.ad_ids)['utm_campaign']
    my_list.append([entry.session_id, utm])

import pandas as pd
columns = ['session_id', 'utm']
utm_count = pd.DataFrame(my_list, columns= columns)
utm_count = utm_count.groupby('session_id')['utm'].value_counts().rename_axis(columns).reset_index(name='counts')

tot_list = []

ids_passed=[]
count = 0
for ix,row in utm_count.iterrows():
    if row['session_id'] in ids_passed:
        pass
    else:
        test001 = 0
        test002 = 0
        test003 = 0
        ids_passed.append(row['session_id'])
        new_df =  utm_count.loc[utm_count['session_id']==row['session_id']]
  
        for i,r in new_df.iterrows():
             
            if r['utm']=='test001':
                test001 = r['counts']
            elif r['utm']=='test002':
                test002 = r['counts']
            elif r['utm']=='test003':
                test003 = r['counts']
        mjsn = {'test001':test001,
                'test002':test002,
                'test003':test003,}
        # count = count+1
        # print(row['session_id'] , mjsn)
        ss_id = row['session_id']
        allrows = session.query(qreuz_table).filter_by(session_id=ss_id)
        '''
        for row in allrows:
            row.comment = str(mjsn)
            session.commit()
        '''