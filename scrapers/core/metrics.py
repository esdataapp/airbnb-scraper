import json,sys,time

def log(event,**kv):
    kv['event']=event; kv.setdefault('ts',time.time())
    print(json.dumps(kv,ensure_ascii=False),file=sys.stdout,flush=True)
