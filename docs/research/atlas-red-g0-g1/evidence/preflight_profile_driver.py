import cProfile,json,os,socket,sys,time
from dataclasses import replace
from pathlib import Path
sys.path.insert(0,str(Path.cwd()/'src'))
for k in list(os.environ):
 if any(x in k.upper() for x in ('KEY','TOKEN','SECRET','PASSWORD','CREDENTIAL')): os.environ.pop(k)
def deny(event,args):
 if event in {'socket.connect','socket.getaddrinfo','socket.gethostbyname','socket.sendto'}: raise RuntimeError('OFFLINE_NETWORK_DENIED')
sys.addaudithook(deny)
from atlas.v2.runtime.storage_preflight import qualify_storage_path,StoragePreflightHooksV1
out=Path('/workspace/work/atlas-g1-run')
root=out/'baseline-storage';root.mkdir(exist_ok=True)
r=qualify_storage_path(root,identity_sha256='a'*64)
(out/'baseline-preflight-host.json').write_text(json.dumps(r.as_dict(),indent=2)+'\n')
# Explicit diagnostic disk override permits mechanical workload, not capacity qualification.
h=replace(StoragePreflightHooksV1(),free_bytes=lambda _:10**12,dependencies=lambda: {'scope':'STORAGE_ONLY_NO_QT_RUNTIME_SMOKE'})
p=cProfile.Profile();p.enable()
r=qualify_storage_path(root,identity_sha256='a'*64,hooks=h)
p.disable();p.dump_stats(str(out/'baseline-profile.prof'))
(out/'baseline-preflight-diagnostic.json').write_text(json.dumps(r.as_dict(),indent=2)+'\n')
print(r.status,r.reasons)
import pstats
pstats.Stats(p).strip_dirs().sort_stats('cumtime').print_stats(22)
