# coding=utf-8

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, String, MetaData
from sqlalchemy import inspect
import os
os.chdir(os.path.dirname(__file__))
from MasterConfig import MasterConfigReader
from pathlib import Path
from json import loads,dumps

conf_obj = MasterConfigReader()
host = conf_obj.get("db_config","hostname")
db = conf_obj.get("db_config","db")
user = conf_obj.get("db_config","user")
port = conf_obj.get("db_config","port")
pwd = conf_obj.get("db_config","password")
rdx_file_path = Path(conf_obj.get("data_ingestion_config","rdx_data_path"))

engine = create_engine('postgresql+psycopg2://'+user+':'+pwd+'@'+host+':'+port+'/'+db)
inspector = inspect(engine)
meta = MetaData(engine)
meta.reflect(bind=engine)
Session = sessionmaker(bind=engine)
# print(meta.tables)
Base = declarative_base()

session = Session()

org_metadata = inspector.get_columns('organization')
org_metadata_columns = [i["name"] for i in org_metadata]
# print(org_metadata_columns)
org_table = meta.tables['organization']

with open(rdx_file_path) as fobj:
    sampleData = fobj.read()
sample_Data = loads(sampleData)
insert_columns = []
with engine.connect() as conn:
    for data in sample_Data["organizations"]:
        org_data_dict = dict()
        for key,value in data.items():
            if value == None:
                data[key] = "None"
        for i in org_metadata_columns:
            if i in data.keys():
                org_data_dict.update({i:data[i]})
            else:
                org_data_dict.update({i:"Not Available"})
        insert_columns.append(org_data_dict)
    conn.execute(org_table.insert(),insert_columns)