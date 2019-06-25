from datetime import datetime, timedelta
import pytz
from pytz import timezone
import requests
import zipfile
import csv
import io
from io import StringIO
import pandas as pd
from sqlalchemy import create_engine
import decimal as D

#Per CAISO Posting Schedule, DAM prices are posted at 1pm Pacific Time every day, including weekends/holidays.

#Set this to the number of days you want the start date offset from the rundate (today). 1 = start with tomorrow's prices.
offset=-1

#Set this to the number of days of data you want to try to download
duration=3 

#List the nodes you want to collect data for. Available nodes are listed at CAISO OASIS site under Prices - Locational Marginal Prices.
nodes="TH_NP15_GEN-APND,TH_SP15_GEN-APND,TH_ZP26_GEN-APND"

#Database connection
dbname='dbname'
user='user'
host='127.0.0.1'
password='password'
table='table'

connection=f'postgresql://{user}:{password}@{host}:5432/{dbname}'
engine = create_engine(connection)
	
nowutc = datetime.now(tz=pytz.utc)
now = datetime.now(tz=pytz.utc).astimezone(timezone('US/Pacific'))
tomorrowstart=datetime(now.year,now.month,now.day)+timedelta(offset)
tomorrowend=tomorrowstart+timedelta(duration)
startdatetime=timezone('US/Pacific').localize(tomorrowstart).astimezone(pytz.utc).strftime("%Y%m%dT%H:%M-0000")
enddatetime=timezone('US/Pacific').localize(tomorrowend).astimezone(pytz.utc).strftime("%Y%m%dT%H:%M-0000")

def psql_insert_copy(table, conn, keys, data_iter):
    # gets a DBAPI connection that can provide a cursor
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        if table.schema:
            table_name = '{}.{}'.format(table.schema, table.name)
        else:
            table_name = table.name

        sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
            table_name, columns)
        cur.copy_expert(sql=sql, file=s_buf)



try:
    url=f"http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=1&startdatetime={startdatetime}&enddatetime={enddatetime}&market_run_id=DAM&node={nodes}"
    print("fetching "+url)
    response=requests.get(url)
    if response.status_code!=200:
        print("Error: http response "+str(response.status_code)+" "+response.reason)
    else:
        zip=zipfile.ZipFile(io.BytesIO(response.content))
        for filename in zip.namelist():
            if filename.split(".")[-1]!='csv':
                print("Error: unexpected file type. Filename is "+filename)
            else:
                #Extract and Transform zip files to dataframe
                print("attempting to extract "+filename)
                df=pd.read_csv(zip.open(filename), converters={'MW':D.Decimal})
                print("beginning data model conversion")
                df=df.pivot_table(index=['INTERVALSTARTTIME_GMT','NODE','OPR_HR'], columns='LMP_TYPE', values='MW', aggfunc='first')
                df.reset_index(inplace=True)
                df['PPT']=pd.DatetimeIndex(pd.to_datetime(df.INTERVALSTARTTIME_GMT)).tz_convert('US/Pacific')
                df['date']=df.PPT.dt.date
                df['createdatetime']=nowutc
                df['market']="DAM"
                df['interval']=0
                df=df.drop(columns=["PPT","INTERVALSTARTTIME_GMT"])
                df=df.rename(index=str, columns={"NODE": "pnode", "OPR_HR": "hour", "LMP": "lmp", "MCE": "energy", "MCC": "congestion","MCL": "losses"})
                print("dates retrieved: "+df.date.min().strftime("%m/%d/%Y")+" to "+df.date.max().strftime("%m/%d/%Y"))
                print("nodes retrieved: "+", ".join(df.pnode.unique()))
                mindate=df.date.min().strftime("%m/%d/%Y")
                maxdate=df.date.max().strftime("%m/%d/%Y")
                nodesretrieved=str(df.pnode.unique().tolist()).strip('[]')
               
                #Query database for potentially conflicting records
                query=f'''Select * from {table} where date between '{mindate}' and '{maxdate}' and pnode in ({nodesretrieved}) and market='DAM';'''
                print('executing query')
                pcr=pd.read_sql_query(query,engine, coerce_float=False)
                if pcr.empty:
                    print('query was empty. constructing empty result set')
                    pcr=pd.DataFrame(columns=['congestion', 'createdatetime', 'date', 'energy', 'hour', 'interval','lmp', 'losses', 'market', 'pnode','updatedatetime'])
                
                #Compare database results to newly downloaded data and load new records.
                c=pd.merge(df,pcr, how='left', on=['date','hour','pnode','market'])
                new=c[c['lmp_y'].isnull()]
                new=new.drop(columns=["createdatetime_y","interval_y","lmp_y","energy_y","congestion_y","losses_y"])
                new=new.rename(index=str, columns={"lmp_x":"lmp","energy_x":"energy","congestion_x":"congestion","losses_x":"losses","createdatetime_x":"createdatetime", "interval_x":"interval"})
                new['updatedatetime']=new['createdatetime']
                newrecordcount=new.shape[0]
                print(f'loading {newrecordcount} new records to database')
                new.to_sql(table, engine, method=psql_insert_copy, if_exists='append', index=False)
                print('new records loaded')
                
                #Compare database results to newly downloaded data and update changed records. Load records to temp table, then run update inside database for performance reasons.
                updated=c[c['lmp_y'].notnull()&((c['lmp_x']!=c['lmp_y']) | (c['energy_x']!=c['energy_y'])| (c['congestion_x']!=c['congestion_y'])| (c['losses_x']!=c['losses_y']))]
                updated=updated.drop(columns=["interval_y","lmp_y","energy_y","congestion_y","losses_y","updatedatetime"])
                updated=updated.rename(index=str, columns={"lmp_x":"lmp","energy_x":"energy","congestion_x":"congestion","losses_x":"losses","createdatetime_x":"updatedatetime", "interval_x":"interval", "createdatetime_y":"createdatetime"})       
                updatedrecordcount=updated.shape[0]
                print(f'loading {updatedrecordcount} updated records to database')
                temptable='tempdamupdate'
                droptempsql=f'''DROP TABLE IF EXISTS {temptable}'''
                engine.connect().execute(droptempsql)
                createtempsql=f'''CREATE TABLE public.{temptable}
                                (
                                    pnode character varying(20) COLLATE pg_catalog."default" NOT NULL,
                                    createdatetime timestamp with time zone NOT NULL,
                                    lmp numeric(10,5) NOT NULL,
                                    energy numeric(10,5) NOT NULL,
                                    congestion numeric(10,5) NOT NULL,
                                    losses numeric(10,5) NOT NULL,
                                    date date NOT NULL,
                                    hour smallint NOT NULL,
                                    "interval" smallint NOT NULL,
                                    market character varying(10) COLLATE pg_catalog."default" NOT NULL,
                                    updatedatetime timestamp with time zone
                                );'''
                engine.connect().execute(createtempsql)
                updated.to_sql('tempdamupdate', engine, method=psql_insert_copy, if_exists='append', index=False)
                updatesql='''update caisotest c
                set updatedatetime=t.updatedatetime, lmp=t.lmp, energy=t.energy, congestion=t.congestion, losses=t.losses
                from tempdamupdate t
                where c.pnode=t.pnode and c.date=t.date and c.hour=t.hour and c.interval=t.interval and c.market=t.market;'''
                engine.connect().execute(updatesql)
                print('update complete')
                engine.connect().execute(droptempsql)
                
except BaseException as error:
    print("Error: "+str(error))
