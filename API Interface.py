# -*- coding: utf-8 -*-
"""
Created on Fri Jun 12 11:01:55 2020
Set nodes and start/end dates. Script will fetch data from DAM LMPs, concatenate to one file, 
and return results to source directory as csv. Summary of success/failure will be written to the variable "summary"
@author: Michael Champ
"""

import requests as r
import pandas as pd
import zipfile
import io
import decimal as D
from datetime import datetime,timedelta
from pytz import timezone
import pytz
import time
import xml.etree.ElementTree as ET

nodes=['CLERLKE_6_N012',
       'ZAMORA_1_N001',
       'GYS5X6_7_UNITS-APND',
       'KONOCTI6_6_N001',
       'MADISON_1_N201']

startdate='2020-01-01'
enddate='2020-06-11'

api_delay=5

def getdata(url):
        df=None
        response=r.get(url)
        if response.status_code!=200:
            print("Error: http response "+str(response.status_code)+" "+response.reason)
            message=str(response.status_code)+" "+response.reason+" "+response.content.decode("utf-8") 
        else:
            zip=zipfile.ZipFile(io.BytesIO(response.content))
            for filename in zip.namelist():
                if filename.split(".")[-1]!='csv':
                    print("Error: unexpected file type. Filename is "+filename)
                    tree=ET.parse(zip.open(filename))
                    root = tree.getroot()
                    m='{http://www.caiso.com/soa/OASISReport_v1.xsd}'
                    message=root.find(f'{m}MessagePayload').find(f'{m}RTO').find(f'{m}ERROR').find(f'{m}ERR_DESC').text
                else:
                    #Extract and Transform zip files to dataframe
                    print("attempting to extract "+filename)
                    df=pd.read_csv(zip.open(filename), converters={'MW':D.Decimal})
                    print("beginning data model conversion")
                    df=df.pivot_table(index=['INTERVALSTARTTIME_GMT','NODE','OPR_HR'], columns='LMP_TYPE', values='MW', aggfunc='first')
                    df.reset_index(inplace=True)
                    df['PPT']=pd.DatetimeIndex(pd.to_datetime(df.INTERVALSTARTTIME_GMT)).tz_convert('US/Pacific')
                    df['date']=df.PPT.dt.date
                    message='success'
        return df,message
    
#Cut start and end date up into 30 day lengths, and set appropriate timezone
start=datetime.strptime(startdate, '%Y-%m-%d')
end=datetime.strptime(enddate, '%Y-%m-%d')
segments=[]
segment_start=start
print("Time slices:")
while (end-segment_start).days>0:
    segment_length=min((end-segment_start).days+1,30)
    segment_end=segment_start+timedelta(days=segment_length)-timedelta(hours=1)
    print(segment_start.strftime("%Y%m%dT%H:%M-0000")+" to "+segment_end.strftime("%Y%m%dT%H:%M-0000"))
    pair=(segment_start,segment_end)
    segments.append(pair)
    segment_start=segment_end+timedelta(hours=1)   
    

results = pd.DataFrame()
summary =  []
for node in nodes:
    for segment in segments:
            print(node,segment[0],segment[1])
            startdatetime=timezone('US/Pacific').localize(segment[0]).astimezone(pytz.utc).strftime("%Y%m%dT%H:%M-0000")
            enddatetime=timezone('US/Pacific').localize(segment[1]).astimezone(pytz.utc).strftime("%Y%m%dT%H:%M-0000")
            url=f"http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime={startdatetime}&enddatetime={enddatetime}&market_run_id=DAM&node={node.upper()}"
            print(f"fetching data from {url}")
            attempt=1
            while attempt<=3:
                response=getdata(url)
                df=response[0]
                msg=response[1]
                delay=5**attempt
                print(f'waiting {delay} seconds for API')
                time.sleep(delay)
                print("resuming")
                if msg[:3]!='429':
                    attempt=99
                else:
                    summary.append([node,startdatetime,enddatetime,msg])
                    attempt=attempt+1
            results=results.append(df)
            summary.append([node,startdatetime,enddatetime,msg])
            print(f"results appended for {node} from {startdatetime} to {enddatetime}")

results.to_csv('results.csv') 
log = pd.DataFrame(summary, columns = ['node', 'start','end','message'])
log.to_csv('log.csv')      
        