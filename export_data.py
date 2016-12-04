import json, sqlalchemy, io, random
from datetime import datetime
from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from config import conn_string

def page_query(q):
	""" Page through query to prevent killed queries """
	
    offset = 0
    while True:
        r = False
        for elem in q.limit(1000).offset(offset):
           r = True
           yield elem
        offset += 1000
        if not r:
            break

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    
    if isinstance(obj, datetime):
        serial = obj.isoformat()
        return serial
    raise TypeError ("Type not serializable")

engine = create_engine(conn_string)
db_conn = engine.connect()
metadata = MetaData(engine)
Session = sessionmaker(bind=engine)
session = Session()

episodes_flat = Table('episodes_flat', metadata, autoload=True)

select = session.query(episodes_flat)

fh = io.open('data/output.json', 'wb')

for row in page_query(select):
	row_dict = dict(zip(row.keys(), row))
	fh.write(json.dumps(row_dict, default=json_serial) + "\n")

fh.close()
