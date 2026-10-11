"""Same-seed repeatability probe on deterministic synthetic bars only."""
from __future__ import annotations
import argparse, json, platform, sys, time
from pathlib import Path
import numpy as np
import pandas as pd
import torch

p=argparse.ArgumentParser()
p.add_argument('--source',type=Path,required=True)
p.add_argument('--model',type=Path,required=True)
p.add_argument('--tokenizer',type=Path,required=True)
a=p.parse_args()
sys.path.insert(0,str(a.source))
from model import Kronos, KronosPredictor, KronosTokenizer
torch.set_num_threads(2)
started=time.perf_counter_ns()
tokenizer=KronosTokenizer.from_pretrained(str(a.tokenizer),local_files_only=True)
model=Kronos.from_pretrained(str(a.model),local_files_only=True)
model.eval(); tokenizer.eval()
load_ns=time.perf_counter_ns()-started
predictor=KronosPredictor(model,tokenizer,device='cpu',max_context=128)
rng=np.random.default_rng(20261011)
close=100.0*np.exp(np.cumsum(rng.normal(0.0,0.001,size=128)))
open_=np.r_[close[0],close[:-1]]
high=np.maximum(open_,close)*1.001
low=np.minimum(open_,close)*0.999
volume=rng.uniform(1.0,10.0,size=128)
frame=pd.DataFrame({'open':open_,'high':high,'low':low,'close':close,'volume':volume})
x=pd.Series(pd.date_range('2025-01-01T00:00:00Z',periods=128,freq='15min'))
y=pd.Series(pd.date_range(x.iloc[-1]+pd.Timedelta(15,unit='min'),periods=3,freq='15min'))
predictions=[]; durations=[]
for _ in range(3):
    torch.manual_seed(20261011)
    t=time.perf_counter_ns()
    out=predictor.predict(frame,x,y,pred_len=3,T=1.0,top_p=0.9,sample_count=1,verbose=False)
    durations.append(time.perf_counter_ns()-t)
    predictions.append(out.to_numpy(dtype=float).tolist())
arr=np.asarray(predictions)
result={'python':platform.python_version(),'torch':torch.__version__,'device':'cpu','threads':torch.get_num_threads(),'seed':20261011,'repeats':len(predictions),'load_elapsed_ns':load_ns,'inference_elapsed_ns':durations,'same_seed_exactly_equal':bool(np.array_equal(arr[0],arr[1]) and np.array_equal(arr[0],arr[2])),'max_abs_repeat_delta':float(np.max(np.abs(arr-arr[0]))),'predictions':predictions,'evidence_class':'SYNTHETIC_REPEATABILITY_NOT_MARKET_OR_ECONOMIC_EVIDENCE'}
print(json.dumps(result,sort_keys=True))
