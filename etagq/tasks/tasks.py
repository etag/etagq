from celery.task import task
from dockertask import docker_task
from subprocess import call,STDOUT
import requests
import json
import os
import sys
import pandas as pd
from datetime import datetime
#Default base directory 
basedir="/data/static/"
host_id='10.195.67.43'

#Example task
@task()
def add(x, y):
    result = x + y
    return result

headers={'Authorization':'Token eb35d2d358594a00afafac51c6e75cf267c689e7'}
def check_tag_id(reader_id,tagid,timestamp):
	print('checking tag')
	payload={'format':'json','tag_id':tagid}
	r1=requests.get('http://{0}/api/etag/tags/'.format(host_id),params=payload,headers=headers)
	print(reader_id)
	print(tagid)
	print(timestamp)
	count1=r1.json()['count']
	if count1 >=1:
		print('tag exists')
		#headers={'Authorization':'Token eb35d2d358594a00afafac51c6e75cf267c689e7'}
		payload={'reader':reader_id,'tag':tagid,'tag_timestamp':timestamp}
		r2=requests.post('http://{0}/api/etag/tag_reads/?format=json'.format(host_id),data=payload,headers=headers)
		print(r2.status_code)
		print(r2.text)
		return 'Files successufully uploaded'	
	else:
		print('Tag doesnot exists')
		payload={'tag_id':tagid,'name':'nilutpal sundi','description':'Sending data to web server'}
		r3=requests.post('http://{0}/api/etag/tags/'.format(host_id),data=payload,headers=headers)
		payload={'reader':reader_id,'tag':tagid,'tag_timestamp':timestamp}
		r4=requests.post('http://{0}/api/etag/tag_reads/?format=json'.format(host_id),data=payload,headers=headers) 
		return 'Files successufully uploaded'
		

def try_data_db(reader_id,file_path):
	print('inside try data')
	data=pd.read_csv(file_path,sep=' ',skiprows=1)
	#data

	columnnames=["TagID","Date","Time"]
	data.columns=columnnames
	data['timestmp']=data.Date.str.cat(data.Time)
	#df['Period'] = df.Year.str.cat(df.Quarter)
	data1=data[['TagID','timestmp']]
	time=data1['timestmp']
	i=0
	length=len(data1.index)
	for i in range(length):
		time[i]= datetime.strptime(time[i],"%m/%d/%y%H:%M:%S")
		#newdtime[i]=dtime
		#dtime=dtime.isoformat().replace('T',' 
		#time
	data1['Timestamp']=time
	data2=data1[['TagID','Timestamp']]
	i=0
	for i in range(length):
		tagid=data2.iloc[i]['TagID']
		timestamp=data2.iloc[i]['Timestamp']
		check_tag_id(reader_id,tagid,timestamp)
	return 'Files successufully uploaded'	
    
@task()
def checkUpload(reader_id,file_path):
	#file_path='/home/etag/Documents/1G02DATA.TXT'	
	headers={'Authorization':'Token eb35d2d358594a00afafac51c6e75cf267c689e7'}
	payload = {'format':'json','reader_id':reader_id}
	r = requests.get('http://{0}/api/etag/readers/'.format(host_id),params=payload,headers=headers)
	print(r.url)
	print(r.json())
	#print(r.json()['count'])
	count=r.json()['count']
	if count >= 1:
		print('count exists')
		try_data_db(reader_id,file_path)
	else:
		print('Reader doesnot exists')
		
	return 'Files successufully uploaded' 

def check_reader(Reader_ID):
		Reader_ID='1G02DATA'
		headers={'Authorization':'Token <token>'}
		payload = {'format':'json','reader_id': '12005'}
		r=requests.get("http://head.ouetag.org/api/etag/readers/",params=payload,headers=headers)
		r.json()
               
#checkUpload(12009,'/home/etag/Documents/4G01DATA.TXT')	
