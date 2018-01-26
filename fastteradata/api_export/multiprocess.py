import sys
import subprocess

def call_sub(f):
    print(f"Calling Fast Export on file...  {f}")
    sys.stdout.flush()
    p1= subprocess.Popen(f"fexp < {f}", shell=True, stdout=subprocess.PIPE)
    print(p1.stdout)
    sys.stdout.flush()
    return("")
