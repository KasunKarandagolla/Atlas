"""Re-run the original fifteen red cases against a clean isolated starting checkout."""
import argparse
import ast
import os
import subprocess
import sys
import tempfile
from pathlib import Path

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--baseline-root',type=Path,required=True)
parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args()
base=args.baseline_root.resolve()
sha=subprocess.check_output(['git','rev-parse','HEAD'],cwd=base,text=True).strip()
assert sha=='7ee561210180f9e0ffa3297a520d35e4857bfe98','Wrong baseline SHA'
assert not subprocess.check_output(['git','status','--porcelain'],cwd=base,text=True),'Baseline must be clean'
root=Path(__file__).resolve().parents[3]
source=(root/'tests/v2/test_full_cycle_integrity.py').read_text()
names={'test_published_bonk_cannot_be_prospective','test_policy_changes_action_identity',
       'test_missing_policy_refused','test_wrong_stop_side_rejected','test_applicability_policy_survives_reopen',
       'test_conflicting_applicability_refused','test_conflicting_raw_receipt_refused',
       'test_ml_purge_outcome_crosses_boundary','test_ml_repeated_opportunities_do_not_cross_folds',
       'test_l2_challenger_is_mean_objective','test_maturity_refuses_gaps_and_early_evaluation',
       'test_unknown_funding_not_silent_zero','test_stop_exit_records_actual_exposure_end',
       'test_discovery_proxy_never_claims_canonical_score'}
lines=source.splitlines(keepends=True)
header='''from decimal import Decimal as D
import inspect
import pytest
from atlas.v2.research.full_cycle import actions, applicability, ml, analysis
from atlas.v2.research.full_cycle.store import ResearchStoreV1
from tests.v2.test_atlas_red_full_cycle import _action, _bar
NS=900_000_000_000
T=1_792_425_600_000_000_000
'''
for node in ast.parse(source).body:
    if isinstance(node,ast.FunctionDef) and node.name in names:
        start=min([node.lineno]+[d.lineno for d in node.decorator_list])-1
        header+='\n'+''.join(lines[start:node.end_lineno])+'\n'
with tempfile.TemporaryDirectory(prefix='atlas-red-probe-') as own:
    test=Path(own)/'test_red_probe.py';test.write_text(header)
    env=dict(os.environ)
    env['PYTHONPATH']=os.pathsep.join(str(base/p) for p in ('src','tests','.'))
    with args.output.open('w') as output:
        result=subprocess.run([sys.executable,'-m','pytest','-q','-o','addopts=','-p','no:cacheprovider',str(test)],
                              cwd=base,env=env,stdout=output,stderr=subprocess.STDOUT)
    text=args.output.read_text()
    assert result.returncode==1 and '15 failed' in text,text[-2000:]
print('Verified: fifteen failures against exact untouched candidate; baseline checkout preserved.')
