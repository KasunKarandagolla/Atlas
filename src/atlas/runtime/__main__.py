from __future__ import annotations

import argparse

from atlas.domain.capability import initial_unverified_fixture

from .connectivity import PrivateVerification, PublicVenueHealth
from .prerequisites import IdentityExpectation, ObservedAccountState
from .safe_runtime import SafeRuntime, SafeRuntimeConfig


def main(argv=None)->int:
    p=argparse.ArgumentParser();p.add_argument('--journal',default='./atlas-journal.db');p.add_argument('--lock',default='./atlas-writer.lock');p.add_argument('--status',default='./atlas-status.json');p.add_argument('--run-forever',action='store_true');a=p.parse_args(argv)
    cap=initial_unverified_fixture();cfg=SafeRuntimeConfig(a.journal,a.lock,a.status,cap.contract_hash(),False,False,IdentityExpectation(),ObservedAccountState('testnet','BYBIT','linear','one_way','isolated','REQUIRED',('BTCUSDT','ETHUSDT'),False),PublicVenueHealth(),PrivateVerification(),5_000_000_000,capability_contract=cap)
    r=SafeRuntime(cfg)
    try:
        result=r.start();print(f'state={result.state.value} new_risk_allowed={result.new_risk_allowed}');print('NO ORDERS SUBMITTED')
        if a.run_forever:r.run_forever()
        else:r.shutdown()
        return 0
    except Exception as e:print(f'BOOT FAILED: {e}');r.shutdown();return 1
if __name__=='__main__':raise SystemExit(main())
