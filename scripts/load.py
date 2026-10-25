"""HTTP acceptance benchmark; does not measure asynchronous completion throughput."""
import argparse,concurrent.futures,json,time,urllib.request,uuid,math
p=argparse.ArgumentParser();p.add_argument('--requests',type=int,default=100);p.add_argument('--concurrency',type=int,default=10);a=p.parse_args()
if a.requests<1 or a.concurrency<1:p.error('Values must be positive')
def send(_):
 body=json.dumps({'orderId':str(uuid.uuid4()),'sku':'LOAD-UNKNOWN-SKU','quantity':1}).encode();start=time.perf_counter()
 try:
  with urllib.request.urlopen(urllib.request.Request('http://localhost:8080/api/orders',data=body,headers={'Content-Type':'application/json'}),timeout=15) as r:ok=r.status==202
 except Exception:ok=False
 return time.perf_counter()-start,ok
start=time.perf_counter()
with concurrent.futures.ThreadPoolExecutor(max_workers=a.concurrency) as pool:results=list(pool.map(send,range(a.requests)))
elapsed=time.perf_counter()-start;latencies=sorted(x[0] for x in results)
print(json.dumps({'requests':a.requests,'concurrency':a.concurrency,'http_accepted':sum(x[1] for x in results),'errors':sum(not x[1] for x in results),'elapsed_seconds':round(elapsed,3),'http_requests_per_second':round(a.requests/elapsed,2),'http_p95_ms':round(latencies[math.ceil(.95*len(latencies))-1]*1000,2),'scope':'HTTP acceptance only; asynchronous business completion is not measured'},indent=2))
