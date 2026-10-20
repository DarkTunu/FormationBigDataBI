"""Container-only integration runner. Never equate reachable ports with data integration.

The default core suite runs independently. --full REQUIRES every optional product;
missing services fail rather than silently skipping. Reports are always written.
"""
import argparse, csv, json, os, subprocess, sys, time, uuid
from datetime import datetime, timezone
from pathlib import Path
from decimal import Decimal
import boto3, requests
from botocore.config import Config
from confluent_kafka import Producer, Consumer

RESULTS=[]
SESSION=requests.Session()
RUN=uuid.uuid4().hex

def request(method,url,**kw):
 r=SESSION.request(method,url,timeout=30,**kw); r.raise_for_status(); return r

def retry(fn,seconds=300):
 deadline=time.monotonic()+seconds
 while True:
  try: return fn()
  except Exception:
   if time.monotonic()>=deadline: raise
   time.sleep(3)

def check(name,fn):
 start=time.monotonic()
 try:
  detail=fn(); RESULTS.append({'test':name,'status':'PASS','detail':str(detail or 'OK'),'seconds':round(time.monotonic()-start,2)})
 except Exception as exc:
  RESULTS.append({'test':name,'status':'FAIL','detail':str(exc),'seconds':round(time.monotonic()-start,2)})
 print(json.dumps(RESULTS[-1],ensure_ascii=False),flush=True)

def http(url):
 response=request('GET',url)
 if url=='http://trino:8080/v1/info' and response.json().get('starting',True):
  raise RuntimeError('Trino is still starting; SQL catalogs are not ready')
 return response.status_code

def trino(sql):
 r=request('POST','http://trino:8080/v1/statement',data=sql,headers={'X-Trino-User':'formation'}).json(); rows=[]
 deadline=time.monotonic()+180
 while True:
  if 'error' in r: raise RuntimeError(r['error'])
  rows+=r.get('data',[])
  if 'nextUri' not in r:return rows
  if time.monotonic()>deadline:raise TimeoutError('Trino query deadline')
  r=request('GET',r['nextUri']).json()

def spark_job(name):
 path='/opt/spark/jobs/'+name
 output=Path('/reports')/(RUN+'-'+name+'.log')
 with output.open('w') as log:
  proc=subprocess.run(['/opt/spark/bin/spark-submit','--conf','spark.driver.host=integration-check','--conf','spark.driver.bindAddress=0.0.0.0',path],stdout=log,stderr=subprocess.STDOUT,timeout=900)
 if proc.returncode:raise RuntimeError(f'{name} exit {proc.returncode}; consulter {output.name}')
 return output.name

def publish_sources():
 sys.path.insert(0, '/producers/bootstrap')
 from kafka_ready import wait_ready
 from confluent_kafka.admin import AdminClient
 wait_ready(AdminClient({'bootstrap.servers':'kafka:19092'}), ['sales.raw','customers.raw','catalog.raw','opendata.raw','application.logs','web.logs.raw'])
 env=dict(os.environ,DATA_DIR='/opt/spark/donnees/input',LOG_DATA_DIR='/opt/spark/donnees/logs',KAFKA_BOOTSTRAP_SERVERS='kafka:19092',PGHOST='postgres-source',PGDATABASE=os.environ['POSTGRES_DB'],PGUSER=os.environ['POSTGRES_USER'],PGPASSWORD=os.environ['POSTGRES_PASSWORD'])
 for name in ['file-producer','postgres-producer','web-log-producer']:
  subprocess.run([sys.executable,f'/producers/{name}/producer.py'],env=env,check=True,timeout=180)
 # The existing OpenData file feeds Kafka even in core mode, so no external API is needed.
 payload=json.loads(Path('/opt/spark/donnees/opendata/sample.json').read_text())
 events=payload if isinstance(payload,list) else payload.get('events',payload.get('records',[]))
 assert events,'No OpenData events in existing file'
 producer=Producer({'bootstrap.servers':'kafka:19092'})
 for event in events:producer.produce('opendata.raw',value=json.dumps(event).encode())
 assert producer.flush(60)==0
 return 'CSV, JSON, XLSX, PostgreSQL, access/application logs, OpenData -> Kafka'

def kafka_roundtrip():
 row=dict(next(csv.DictReader(Path('/opt/spark/donnees/input/sales.csv').open())))
 for key in ['sale_id','customer_id','product_id','quantity']: row[key]=int(row[key])
 for key in ['unit_price','discount','amount']: row[key]=float(row[key])
 row['integration_run']=RUN
 p=Producer({'bootstrap.servers':'kafka:19092'})
 c=Consumer({'bootstrap.servers':'kafka:19092','group.id':'integration-'+RUN,'auto.offset.reset':'earliest','enable.auto.commit':False})
 c.subscribe(['sales.raw'])
 errors=[]
 p.produce('sales.raw',value=json.dumps(row).encode(),on_delivery=lambda e,m:errors.append(str(e)) if e else None)
 assert p.flush(30)==0 and not errors,errors
 deadline=time.monotonic()+120
 try:
  while time.monotonic()<deadline:
   msg=c.poll(1)
   if msg is None:continue
   if msg.error():raise RuntimeError(msg.error())
   value=json.loads(msg.value())
   if value.get('integration_run')==RUN:
    assert value['sale_id']==row['sale_id'];return 'existing sale row returned exactly'
  raise TimeoutError('Kafka event not consumed')
 finally:c.close()

def s3():
 return boto3.client('s3',endpoint_url='http://objectstore:9000',aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],region_name=os.environ['AWS_REGION'],config=Config(s3={'addressing_style':'path'}))

def s3_roundtrip():
 client=s3();body=Path('/opt/spark/donnees/input/sales.csv').read_bytes();key='integration/'+RUN+'/sales.csv'
 try:
  client.put_object(Bucket='lakehouse',Key=key,Body=body)
  assert client.get_object(Bucket='lakehouse',Key=key)['Body'].read()==body
 finally:client.delete_object(Bucket='lakehouse',Key=key)
 return 'sales.csv exact byte comparison'

SALES=list(csv.DictReader(Path('/opt/spark/donnees/input/sales.csv').open()))
EXPECTED=len(SALES)
AMOUNT=sum(Decimal(r['amount']) for r in SALES)
def verify_trino():
 rows=trino('SELECT count(*), sum(amount) FROM iceberg.gold.sales_detail')
 assert rows[0][0]==EXPECTED,rows
 assert abs(Decimal(str(rows[0][1]))-AMOUNT)<Decimal('0.02'),rows
 pg=trino('SELECT count(*) FROM postgresql.public.customers')
 assert pg[0][0]>=5,pg
 join=trino('SELECT count(*) FROM iceberg.gold.sales_detail s JOIN postgresql.public.customers c ON s.customer_id=c.customer_id')
 assert join[0][0]>0,join
 return f'CSV count={EXPECTED}, sum={AMOUNT}; federation PostgreSQL/Iceberg verified'

def druid_sql(sql):return request('POST','http://druid-router:8888/druid/v2/sql',json={'query':sql,'context':{'useApproximateCountDistinct':False}}).json()
def verify_druid():
 client=s3();key='input/sales.csv';client.upload_file('/opt/spark/donnees/input/sales.csv','druid',key)
 dims=['sale_id','customer_id','country','product_id','product_name','category']
 schema={'dataSource':'training_sales','timestampSpec':{'column':'event_ts','format':'auto'},'dimensionsSpec':{'dimensions':dims},'metricsSpec':[{'type':'doubleSum','name':'amount','fieldName':'amount'}],'granularitySpec':{'type':'uniform','segmentGranularity':'DAY','queryGranularity':'NONE','rollup':False}}
 body={'type':'index_parallel','id':'integration-batch-'+RUN,'spec':{'dataSchema':schema,'ioConfig':{'type':'index_parallel','inputSource':{'type':'s3','uris':['s3://druid/input/sales.csv']},'inputFormat':{'type':'csv','findColumnsFromHeader':True},'appendToExisting':False},'tuningConfig':{'type':'index_parallel','maxNumConcurrentSubTasks':1}}}
 task=request('POST','http://druid-router:8888/druid/indexer/v1/task',json=body).json()['task']
 deadline=time.monotonic()+600
 while True:
  state=request('GET',f'http://druid-router:8888/druid/indexer/v1/task/{task}/status').json()['status']['status']
  if state=='SUCCESS':break
  if state=='FAILED':raise RuntimeError('Druid ingestion FAILED: '+task)
  if time.monotonic()>deadline:raise TimeoutError('Druid ingestion deadline: '+task)
  time.sleep(3)
 def counted():
  result=druid_sql('SELECT COUNT(*) AS n, SUM(amount) AS amount FROM training_sales')[0]
  assert result['n']==EXPECTED,result
  assert abs(Decimal(str(result['amount']))-AMOUNT)<Decimal('0.02'),result
  return result
 result=retry(counted,600)
 # Supervisor persists as a teaching example. It reads actual sales.raw events.
 stream=json.loads(json.dumps(schema));stream['dataSource']='training_sales_stream'
 spec={'type':'kafka','spec':{'dataSchema':stream,'ioConfig':{'type':'kafka','topic':'sales.raw','consumerProperties':{'bootstrap.servers':'kafka:19092'},'inputFormat':{'type':'json'},'useEarliestOffset':True,'taskCount':1,'replicas':1,'taskDuration':'PT10M'},'tuningConfig':{'type':'kafka'}}}
 request('POST','http://druid-router:8888/druid/indexer/v1/supervisor',json=spec)
 def consumed():
  out=druid_sql('SELECT COUNT(DISTINCT sale_id) AS n FROM training_sales_stream')[0]
  assert out['n']>=EXPECTED,out
  return out
 stream_result=retry(consumed,600)
 assert client.list_objects_v2(Bucket='druid',Prefix='segments/').get('KeyCount',0)>0
 return {'batch':result,'kafka':stream_result,'deep_storage':'RustFS S3 segments present'}

def verify_opendata_api():
 payload=request('GET','http://mock-opendata:8000/api/events').json()
 expected=payload['records'][0]
 consumer=Consumer({'bootstrap.servers':'kafka:19092','group.id':'api-check-'+RUN,'auto.offset.reset':'earliest','enable.auto.commit':False})
 consumer.subscribe(['opendata.raw'])
 deadline=time.monotonic()+120
 try:
  while time.monotonic()<deadline:
   msg=consumer.poll(1)
   if msg is None:continue
   if msg.error():raise RuntimeError(msg.error())
   record=json.loads(msg.value())
   if record.get('source')=='http://mock-opendata:8000/api/events' and record.get('payload')==expected:return 'existing API record found in Kafka with source envelope'
  raise TimeoutError('api-producer event missing')
 finally:consumer.close()


def verify_superset():
 session=requests.Session();base='http://superset:8088'
 login=session.post(base+'/api/v1/security/login',json={'username':os.environ['SUPERSET_ADMIN_USER'],'password':os.environ['SUPERSET_ADMIN_PASSWORD'],'provider':'db','refresh':True},timeout=30);login.raise_for_status()
 session.headers['Authorization']='Bearer '+login.json()['access_token']
 csrf=session.get(base+'/api/v1/security/csrf_token/',timeout=30);csrf.raise_for_status()
 session.headers['X-CSRFToken']=csrf.json()['result'];session.headers['Referer']=base+'/'
 response=session.get(base+'/api/v1/database/',timeout=30);response.raise_for_status()
 databases={d['database_name']:d['id'] for d in response.json()['result']}
 queries={'PostgreSQL formation':'SELECT COUNT(*) AS n FROM public.customers','Trino Lakehouse':'SELECT COUNT(*) AS n FROM iceberg.gold.sales_detail','Apache Druid':'SELECT COUNT(*) AS n FROM training_sales'}
 counts={}
 for name,sql in queries.items():
  response=session.post(base+'/api/v1/sqllab/execute/',json={'database_id':databases[name],'sql':sql,'runAsync':False,'expand_data':True,'json':True,'queryLimit':1000},timeout=180);response.raise_for_status()
  body=response.json()
  assert body.get('status')=='success',body
  count=int(body['data'][0]['n']);assert (count==EXPECTED if name!='PostgreSQL formation' else count>=5),(name,count)
  counts[name]=count
 return counts

def write_report(full):
 failed=any(r['status']=='FAIL' for r in RESULTS)
 report={'run':RUN,'finished_at':datetime.now(timezone.utc).isoformat(),'mode':'full' if full else 'core','status':'FAIL' if failed else 'PASS','results':RESULTS}
 Path('/reports/integration.json').write_text(json.dumps(report,ensure_ascii=False,indent=2))
 return 1 if failed else 0

def main():
 parser=argparse.ArgumentParser();parser.add_argument('--full',action='store_true');args=parser.parse_args()
 Path('/reports').mkdir(exist_ok=True)
 for name,url in [('PostgreSQL via Trino','http://trino:8080/v1/info'),('Kafka UI','http://kafka-ui:8080'),('pgAdmin','http://pgadmin:80/misc/ping'),('RustFS console','http://objectstore:9001/rustfs/console/health'),('Iceberg REST','http://iceberg-rest:8181/v1/config'),('Spark Master','http://spark-master:8080'),('Spark Worker 1','http://spark-worker-1:8081'),('Spark Worker 2','http://spark-worker-2:8081'),('JupyterLab','http://spark-jupyter:8888/login'),('Dashboard','http://dashboard:8090/health')]:
  check(name,lambda url=url:retry(lambda:http(url),180))
 if args.full:
  for name,url in [('Airflow','http://airflow:8080/api/v2/monitor/health'),('Druid console','http://druid-router:8888'),('Superset','http://superset:8088/health'),('OpenData API','http://mock-opendata:8000/api/events')]:
   check(name,lambda url=url:retry(lambda:http(url),300))
 if any(r['status']=='FAIL' for r in RESULTS):return write_report(args.full)
 check('Sources existantes -> Kafka',publish_sources)
 check('Kafka producer -> consumer',kafka_roundtrip)
 check('RustFS S3 read/write',s3_roundtrip)
 for name in ['10_verify_workspace_s3.py','00_runtime_smoke.py','01_batch_to_parquet.py','02_kafka_to_delta.py','03_build_iceberg_gold.py','04_ml_kmeans.py','05_kafka_aux_to_parquet.py','06_kafka_web_logs_to_delta.py','07_delta_web_logs_to_iceberg.py','08_web_logs_ml_kmeans.py','09_verify_existing_data.py']:
  check('Spark '+name,lambda name=name:spark_job(name))
 check('Trino -> PostgreSQL + Iceberg + S3',lambda:retry(verify_trino,180))
 def history():
  apps=request('GET','http://spark-history:18080/api/v1/applications').json();assert apps;return len(apps)
 check('Spark event logs S3 -> History',lambda:retry(history,180))
 if args.full:
  check('OpenData API -> api-producer -> Kafka',verify_opendata_api)
  check('S3 + Kafka -> Druid + S3 segments',verify_druid)
  check('Superset SQL -> PostgreSQL + Trino + Druid',verify_superset)
 return write_report(args.full)
if __name__=='__main__':sys.exit(main())
