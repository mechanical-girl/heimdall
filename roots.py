import json
import time
import os
import subprocess
import sys

try:
    with open("roots.json", "r") as f:
        root_pid = json.loads(f.read())

    result = subprocess.run(['ps', f"{root_pid}"], stdout=subprocess.PIPE)
    pid = result.stdout.decode('utf-8').split('\n')[1]
    if pid != '':
        sys.exit(0)

except FileNotFoundError:
    with open("roots.json", "w") as f:
        f.write('')

with open(os.devnull, 'w') as f:
    proc = subprocess.Popen(['pipenv', 'run', 'python', 'yggdrasil.py'])
with open("roots.json", "w") as f:
    f.write(json.dumps(proc.pid))
