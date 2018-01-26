import subprocess
import sys
import shlex
import errno
import os
import pty
import select
import signal

def return_total_data_blocks (strng):
    strng = str(strng)
    block_cnt = 0
    if int(str(strng).find("Select execution completed")) > 0:
        find_1 = str(strng).find(".")
        find_2 = str(strng)[int(find_1)+2:].find(" ")
        print(find_1)
        print(find_2)
        print(strng[find_1+2:len(str(strng)[int(find_1):])-(find_2+find_1+2)])
        block_cnt = int(strng[find_1+2:len(str(strng)[int(find_1):])-(find_2+find_1+2)])
        print(str(block_cnt) + ' blocks total')
        print("You will have a more complete idea of timing after 5 minutes")
    else:
        block_cnt = 0
    return block_cnt

def return_current_data_blocks(strng):
    #UTY8732 26263.60 blocks processed per minute (running average).
    strng = str(strng)
    block_cnt = 0
    if int(str(strng).find("(running average)")) > 0:
        find_1 = str(strng).find(" ")
        find_2 = str(strng)[int(find_1)+2:].find(" ")
        block_cnt = int(strng[find_1+2:len(str(strng)[int(find_1):])-(find_2+find_1+2)])
        print(str(block_cnt) + ' blocks average')
    else:
        block_cnt = 0
        return (block_cnt*5)

# Includes code borrowed from: Tobias Brink
# http://tbrink.science/blog/2017/04/30/processing-the-output-of-a-subprocess-with-python-in-realtime/

class OutStream:
        def __init__(self, fileno):
            self._fileno = fileno
            self._buffer = ""

        def read_lines(self):
            try:
                output = os.read(self._fileno, 1000).decode()
            except OSError as e:
                if e.errno != errno.EIO: raise
                output = ""
            lines = output.split("\n")
            lines[0] = self._buffer + lines[0] # prepend previous
                                               # non-finished line.
            if output:
                self._buffer = lines[-1]
                return lines[:-1], True
            else:
                self._buffer = ""
                if len(lines) == 1 and not lines[0]:
                    # We did not have buffer left, so no output at all.
                    lines = []
                return lines, False

        def fileno(self):
            return self._fileno

signal.signal(signal.SIGINT, lambda s,f: print("received SIGINT"))

def run_process(fexp, progress):
        print(fexp)
        # Start the subprocess.
        out_r, out_w = pty.openpty()
        err_r, err_w = pty.openpty()
        proc = subprocess.Popen([fexp], shell=True, stdout=out_w, stderr=err_w)
        os.close(out_w) # if we do not write to process, close these.
        os.close(err_w)

        fds = {OutStream(out_r), OutStream(err_r)}
        while fds:
        # Call select(), anticipating interruption by signals.
            while True:
                try:
                    rlist, _, _ = select.select(fds, [], [])
                    break
                except InterruptedError:
                    continue
            # Handle all file descriptors that are ready.
            for f in rlist:
                lines, readable = f.read_lines()
                # Example: Just print every line. Add your real code here.
                current = 0
                bar_start = 0
                total = 0
                for line in lines:
                    print(line)
                    total = return_total_data_blocks(line)
                    if total > 0:
                        bar_start = 1
                    if bar_start == 1:
                        current = return_current_data_blocks(line)+current
                        bar.update(current)
                if not readable:
                    # This OutStream is finished.
                    fds.remove(f)
        return

def call_sub(f, step_detail):
    print(f"Calling Fast Export on file...  {f}")
    sys.stdout.flush()
    if step_detail == False:
        run_process(f"fexp < {f} -s ON",True)
        sys.stdout.flush()
        print("Fast Export Complete")
    else:
        
    return("")
