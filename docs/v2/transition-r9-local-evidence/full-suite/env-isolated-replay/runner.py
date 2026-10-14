import ast
import os
import shlex
import subprocess
import sys
from pathlib import Path

ROOT = Path('/home/kasun/Music/atlas-red-r9')
VENV_PYTHON = '/home/kasun/Music/atlas/.venv/bin/python'
OUT = ROOT / 'docs/v2/transition-r9-local-evidence/full-suite/credential-scrubbed'
SECRET_NAMES = ('GEMINI_API_KEY', 'BINANCE_FUTURES_DEMO_API_KEY', 'BINANCE_FUTURES_DEMO_API_SECRET')


def recorded_command(path: str) -> list[str]:
    for line in Path(path).read_text().splitlines():
        if line.lstrip().startswith('Command being timed: '):
            raw = line.split('Command being timed: ', 1)[1].strip()
            value = ast.literal_eval(raw)
            args = shlex.split(value)
            index = next(i for i in range(len(args) - 1) if args[i] == '-m' and args[i + 1] == 'pytest')
            return args[index + 2:]
    raise ValueError(f'no pytest command in {path}')


def test_paths(args: list[str]) -> list[str]:
    return [x for x in args if x.startswith('tests/') and not x.startswith('--')]


def run(name: str, args: list[str], *, env_updates: dict[str, str] | None = None) -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    junit = OUT / f'{name}-junit.xml'
    console = OUT / f'{name}-console.txt'
    timing = OUT / f'{name}-time.txt'
    cleaned = os.environ.copy()
    for key in SECRET_NAMES:
        cleaned.pop(key, None)
    if env_updates:
        cleaned.update(env_updates)
    normalized = [f'--junitxml={junit}' if x.startswith('--junitxml=') else x for x in args]
    if not any(x.startswith('--junitxml=') for x in normalized):
        normalized.append(f'--junitxml={junit}')
    command = [VENV_PYTHON, '-m', 'pytest', *normalized]
    with console.open('w') as stdout, timing.open('w') as stderr:
        completed = subprocess.run(['/usr/bin/time', '-v', *command], cwd=ROOT, env=cleaned,
                                   stdout=stdout, stderr=stderr, check=False)
        stderr.write(f'\npytest_exit_status={completed.returncode}\n')
    print(f'{name}: pytest_exit_status={completed.returncode}; credential_variables_removed=true; junit={junit.relative_to(ROOT)}')
    return completed.returncode


if __name__ == '__main__':
    mode = sys.argv[1]
    if mode in {'batch-1-final', 'batch-3', 'batch-4'}:
        files = {
            'batch-1-final': 'docs/v2/transition-r9-local-evidence/full-suite/batch-1-final-time.txt',
            'batch-3': 'docs/v2/transition-r9-local-evidence/full-suite/batch-3-time.txt',
            'batch-4': 'docs/v2/transition-r9-local-evidence/full-suite/batch-4-time.txt',
        }
        args = recorded_command(str(ROOT / files[mode]))
        sys.exit(run(mode, args))
    original = recorded_command(str(ROOT / 'docs/v2/transition-r9-local-evidence/full-suite/batch-2-time.txt'))
    paths = test_paths(original)
    production = next(i for i, p in enumerate(paths) if p.endswith('/test_session027_production.py'))
    prefix = paths[:production]
    prefix_args = ['-q', *prefix]
    results = [run('batch-2-prefix', prefix_args, env_updates={'TMPDIR': '/dev/shm/atlas-r9-tests'})]
    production_args = ['-q', 'tests/v2/test_session027_production.py']
    results.append(run('batch-2-production-full', production_args))
    provider_args = recorded_command(str(ROOT / 'docs/v2/transition-r9-local-evidence/full-suite/batch-2-provider-time.txt'))
    results.append(run('batch-2-provider', provider_args))
    suffix_args = recorded_command(str(ROOT / 'docs/v2/transition-r9-local-evidence/full-suite/batch-2-suffix-time.txt'))
    results.append(run('batch-2-suffix', suffix_args))
    sys.exit(1 if any(results) else 0)
