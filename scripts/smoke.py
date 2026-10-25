"""Run after docker compose up. Python standard library only; fails on timeout."""
import json,time,uuid,urllib.request,urllib.error,argparse,os
p=argparse.ArgumentParser();p.add_argument("--core-only",action="store_true");p.add_argument('--grafana-url',default='http://localhost:'+os.environ.get('GRAFANA_PORT','3000'));args=p.parse_args()
def request(url,data=None):
 body=None if data is None else json.dumps(data).encode()
 with urllib.request.urlopen(urllib.request.Request(url,data=body,headers={'Content-Type':'application/json'}),timeout=5) as r:return json.load(r)
def wait(check,seconds=240):
 end=time.monotonic()+seconds;last=None
 while time.monotonic()<end:
  try:
   result=check()
   if result:return result
  except Exception as e:last=e
  time.sleep(1)
 raise AssertionError(f'Timed out: {last}')
for port in [8080,8081]:wait(lambda:request(f'http://localhost:{port}/actuator/health')['status']=='UP')
stock_url='http://localhost:8081/api/inventory/DEMO-SKU';initial=request(stock_url)['available']
assert initial>=1,'Use a demo database with stock remaining'
def order(quantity,sku='DEMO-SKU'):
 body={'orderId':str(uuid.uuid4()),'sku':sku,'quantity':quantity};request('http://localhost:8080/api/orders',body);return body
def state(body):return request('http://localhost:8080/api/orders/'+body['orderId'])['status']
ok=order(1);wait(lambda:state(ok)=='CONFIRMED',60)
request('http://localhost:8080/api/orders',ok);assert request(stock_url)['available']==initial-1
bad=order(1,'MISSING-SKU');wait(lambda:state(bad)=='REJECTED',60)
for body,code in [(dict(ok,quantity=2),409),(dict(ok,orderId=str(uuid.uuid4()),quantity=0),400)]:
 try:request('http://localhost:8080/api/orders',body);raise AssertionError('Invalid request accepted')
 except urllib.error.HTTPError as e:assert e.code==code
for port in [8080,8081]:
 with urllib.request.urlopen(f'http://localhost:{port}/actuator/prometheus') as r:assert b'jvm_memory_used_bytes' in r.read()
if not args.core_only:
 wait(lambda:len([t for t in request('http://localhost:9090/api/v1/targets')['data']['activeTargets'] if t['health']=='up'])==2,60)
 with urllib.request.urlopen(args.grafana_url+'/api/health') as r:assert r.status==200
print('PASS: confirmation, rejection, idempotency, conflict, validation, JVM metrics' + ('' if args.core_only else ', Prometheus and Grafana health'))
