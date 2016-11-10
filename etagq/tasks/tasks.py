from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
import requests
import json
import os, sys
import pandas as pd
from datetime import datetime

#Default base directory 
basedir="/data/static/"

hostname=os.environ.get("host-hostname", '10.195.67.43')

def insert_tag_reads(row,session):
    payload={'format':'json','tag_id':row['TagID']}
    r1=session.get('http://{0}/api/etag/tags/'.format(hostname),params=payload)
    if r1.json()['count'] <1:
        payload={'tag_id':row['TagID'],'name':'ETAG TAG_ID {0}'.format(row['TagID']),'description':'ETAG TAG_ID {0}'.format(row['TagID'])}
        session.post('http://{0}/api/etag/tags/'.format(hostname),data=payload)
    payload={'reader':row['reader_id'],'tag':row['TagID'],'tag_timestamp':row['timestamp']}
    r2=session.post('http://{0}/api/etag/tag_reads/?format=json'.format(hostname),data=payload)
    return r2.status_code

def try_data_db(reader_id,file_path,session):
	print('inside try data')
	data=pd.read_csv(file_path,sep=' ',skiprows=1)
	#data
	columnnames=["TagID","Date","Time"]
	data.columns=columnnames
	data['timestamp']=pd.to_datetime(data['Date'] + ' ' + data['Time'])
        #data.Date.str.cat(data.Time)
	#df['Period'] = df.Year.str.cat(df.Quarter)
	data1=data[['TagID','timestmp']]
        data1['reader_id']=reader_id
        data1.apply( (lambda x: insert_tag_reads(x,session)), axis=1)
        return "Tag Reads recored: {0}".format(len(data1.index))
    
@task()
def etagDataUpload(reader_id,file_path,token):
	#file_path='/home/etag/Documents/1G02DATA.TXT'	
	headers={'Authorization':'Token {0}'.format(token)}
	payload = {'format':'json','reader_id':reader_id}
        s = requests.Session()
	r = s.get('http://{0}/api/etag/readers/'.format(hostname),params=payload,headers=headers)
        if r.json()['count'] >=1:
            return try_data_db(reader_id,file_path,s)
        else:
            raise Exception('reader_id must be provided')
