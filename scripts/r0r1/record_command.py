#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path
import subprocess
import time


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--name', required=True)
    p.add_argument('--timeout', type=int, default=1200)
    p.add_argument('command', nargs=argparse.REMAINDER)
    a = p.parse_args()
    command = a.command[1:] if a.command[:1] == ['--'] else a.command
    root = a.root.resolve(strict=True)
    project = Path(__file__).resolve().parents[2]
    if project not in root.parents or not command or '/' in a.name:
        p.error('invalid project evidence root, label or command')
    record = dict(argv=command, cwd=str(project), start_ns=time.time_ns(), timeout_s=a.timeout)
    with (root / (a.name + '.log')).open('xb') as out:
        try:
            result = subprocess.run(command, cwd=project, stdout=out, stderr=subprocess.STDOUT,
                                    timeout=a.timeout)
            record['returncode'] = result.returncode
        except subprocess.TimeoutExpired:
            record.update(returncode=124, error='execution budget exhausted')
    record['end_ns'] = time.time_ns()
    record['output'] = a.name + '.log'
    for path, mode, text in ((root / (a.name + '-status.json'), 'x', json.dumps(record, indent=2)),
                             (root / 'commands.jsonl', 'a', json.dumps(record))):
        with path.open(mode) as f:
            f.write(text + '\n')
            f.flush()
            os.fsync(f.fileno())
    print(json.dumps(record), flush=True)
    return 0 if record['returncode'] == 0 else 1


if __name__ == '__main__':
    raise SystemExit(main())
