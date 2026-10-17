# Reproduce the historical red cases safely

The saved `red.txt`/XML ran on exact source `215a0045d8203f23dbd4935639ef448556cc2353`, before production edits, when the new module contained four collected cases. The final module adds further green/boundary oracles. To replay the original four assertions with the final fixture helper, copy it into a separate detached parent worktree and select those four cases:

```bash
git worktree add --detach work/p1-bootstrap-parent 215a0045d8203f23dbd4935639ef448556cc2353
cp tests/p1/test_bootstrap_integrity.py work/p1-bootstrap-parent/tests/p1/
cd work/p1-bootstrap-parent
/workspace/.venvs/atlas/bin/python scripts/atlas_p1_offline.py tests/p1/test_bootstrap_integrity.py -k 'rehashed_understatement_rejected or cal_neg_03_connected_evaluator or cal_neg_05_saved_b_refuses' --junitxml=red.xml
```

Expected: four refusal assertions fail, exposing the defect. Do not interpret failures caused by missing new validation modules in other tests as the red oracle. The final correction's complete P1 suite separately checks all green cases.

`parent-numerical-probe.py` contains the exact extra numerical probe used in the historical detached worktree. Append that file to the copied test module, then run `-k parent_numeric_capture`. The recorded command/result has 22 collected, 1 passed and 21 deselected; it confirms that the old connected evaluator admits the exact action at +3 with forged zero allowance even though the actual bound is 3.333333333333333333333333333. Its JSON is written to `/workspace/work/p1-bootstrap/parent-numerical.json`, matching the saved evidence. That output directory must exist. This old permissive oracle belongs only in the detached parent checkout.

No parent source, old evidence, freeze or authority file is edited. Use the existing offline runner and pinned environment. The correction's `reproduce.py` independently regenerates the current capsule, correct financial results and replay/performance evidence.
