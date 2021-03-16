# coding=utf-8

# 1. Imports
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, mapper, clear_mappers
from sqlalchemy import Table, Column, String, MetaData
from sqlalchemy import inspect
import os
os.chdir(os.path.dirname(__file__))
from MasterConfig import MasterConfigReader
from pathlib import Path
from json import loads,dumps

# 2. Loading Configurations
conf_obj = MasterConfigReader()
host = conf_obj.get("db_config","hostname")
db = conf_obj.get("db_config","db")
user = conf_obj.get("db_config","user")
port = conf_obj.get("db_config","port")
pwd = conf_obj.get("db_config","password")
rdx_file_path = Path(conf_obj.get("data_ingestion_config","rdx_data_path"))

# 3. create database url
engine = create_engine('postgresql+psycopg2://'+user+':'+pwd+'@'+host+':'+port+'/'+db)

# 4. Metadata - Generate database schema
metadata = MetaData(bind=engine)

# 5. Inspect - Get database information
inspector = inspect(engine)
org_metadata = inspector.get_columns('organization')
org_model_columns = []
for col in org_metadata:
    if col["name"] == "id":
        pass
    else:
        org_model_columns.append(tuple([col['name'],col['type']]))
org_metadata_columns = [i["name"] for i in org_metadata]

# 6. Session - define ORM
Session = sessionmaker(bind=engine)
Base = declarative_base()

session = Session()

# 7. define data model
class Organization(Base):
    __tablename__ = "Organization"
    id = Column(String, primary_key=True)
    name = Column(String)

# 8. define new model with dynamic columns
new_org_model = Table('organization',meta,Column('id',String,primary_key=True),*(Column(col,type)for col,type in org_model_columns))

# 9. Clear the old mapping
clear_mappers()

# 10. Map the old data model with new model(update)
mapper(Organization, new_org_model)

# 11. Load the json data from expound
with open(rdx_file_path) as fobj:
    sampleData = fobj.read()
sample_Data = loads(sampleData)

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
    # 12. create class instance with expound data
    orgInstance = Organization(**org_data_dict)
    # 13. Insert into postgres db using ORM session
    session.add(orgInstance)
# 14. Commit the changes
session.commit()
# 15. Close the connection
session.close()