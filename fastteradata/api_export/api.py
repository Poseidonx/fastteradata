import sys
import time
import pandas as pd
import numpy as np
import os
import subprocess
import shlex
import errno
import pty
import select
import signal

from joblib import Parallel, delayed

from ..file_processors.file_processors import *
from ..file_processors.io_processors import *
from ..metadata_processors.metadata_processors import *


def extract_table(abs_path, table_name, env, db, nrows=-1, connector = "teradata", columns = [], clean_and_serialize="feather", partition_key="", partition_type="year", primary_keys=[], meta_table="", where_clause="", suppress_text="False", step_detail=False):
    """
        Summary:
            Extracts table information from Teradata and saves / executes the appropriate files

        Args:
            abs_path (str): Absolute path where you want your scripts to reside and data and pickled subdirectories made
            table_name (str): Teradata table name you wish to query
            env (str): Environment that you want to connect to. (People usually have a testing and production environment)
            db (str): Database name to connect to
            nrows (int): *default = -1* The default of -1 means ALL rows. Otherwise, you can specificy a subset of rows such as 20
            connector (str): *default = 'teradata'* The default uses the teradata python module to connect to the cluster. Valid options include 'teradata' and 'pyodbc'
            columns (list(str)): *default = []* Subset of columns to use, default is all of the columns found in the metadata, however subsets can be selected by passing in ['col1','col2','col3']
            clean_and_pickle (bool): *default = True* Refers to if you want to read the resulting data file into memory to clean and then serialize in your pickled subdirectory
            partition_key (str): *default = ''* There is no partitioning by default. When you define a partition key, it MUST BE A DATE COLUMN AS DEFINED IN TERADATA. This breaks up the exporting
                                    into paritions by the *partition_type* argument. This generates multiple fexp scripts and executes them in parrelel using the available cores. This helps to break
                                    up extremely large datasets or increase speed. When a parition key is defined, after all of the partition files are finished loading from Teradata, the resulting data
                                    is COMBINED into a SINGLE DATA FILE and finishes processing through the following cleaning, data type specification, and serializing.
            partition_type (str): *default = 'year'* Default is to partition the partition_key by distict YEAR. Valid options include "year" or "month"
            suppress_text(bool): *default = 'False'* Default is not to suppress the fast export SQL text.
                step_detail(bool): *default = False* Defailt is not to output extract detail from running the export script. This only works on Linux. Cannot select True on Windows. Will add code to auto-detect.
   
        Returns:
            Column list recieved from the metadata if clean_and_pickle is set to False, else nothing. Column names are returned in this case so you can save them and use them to read the raw data file
                later with appropriate columns.
    """
    import time
    import os
    try:
        if not os.path.isdir(f"{abs_path}/data"):
            os.makedirs(f"{abs_path}/data")
        if not os.path.isdir(f"{abs_path}/serialized"):
            os.makedirs(f"{abs_path}/serialized")
    except:
        raise Exception("Oops something went wrong when trying to make your storage directories. \
                            Make sub directories in your absolute path folder of 'data' and 'pickled'")
    data_file, concat_str, data_files, remove_cmd, _df, combine_type = "","","","","", ""
    try:
        t1 = time.time()
        print(f"Starting process for: {db}.{table_name}")
        script_name = table_name
        print("Grabbing meta data and generating fast export file...")
        col_list, fexp_scripts, did_partition, dtype_dict = parse_sql_single_table(abs_path, env,db,table_name, nrows=nrows, connector=connector, columns = columns, partition_key=partition_key, partition_type=partition_type, primary_keys=primary_keys, meta_table=meta_table, where_clause=where_clause, suppress_text=suppress_text)

        #FOR MULTIPROCESSING WHEN PUT INTO A PACKAGE
        from .multiprocess import call_sub

        print("finished")
        #Can only execute in parrelel from command line in windows, won't execute from jupyter notebooks on a windows machine
        #So we only parrelelize if we see we are on linux
        
        # uses code written 2017 by Tobias Brink
        #http://tbrink.science/blog/2017/04/30/processing-the-output-of-a-subprocess-with-python-in-realtime/
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
        
        #!/usr/bin/env python3

        signal.signal(signal.SIGINT, lambda s,f: print("received SIGINT"))
        
        def check_line(line,total_blocks, current_blocks):
            a = ['0001','0002','0003','0004','Select request submitted to the RDBMS.','data blocks generated.','Retrieval Rows statistics:','Elapsed time:','CPU time:','MB/sec:','MB/cpusec:','Total processor time','Start :','End   :','Highest return','running average']
            b = ['running average'] 
            c = ['data blocks generated.']
            if any (x in line for x in a):
                print(line)            
                if any (y in line for y in c):
                    find_1 = line.find('.')
                    find_2 = line[find_1+2:].find(' ')
                    total_blocks = int(line[find_1+2:-(len(line)-find_1-find_2-2)])
                if any (y in line for y in b):
                    find_1 = line.rfind('blocks')-1
                    find_2 = line[:-len(line)+find_1].rfind(' ')
                    current_blocks = (5*int(line[find_2+1:-len(line)+find_1-3])) + current_blocks
                    print('     ' + str("{:,}".format(current_blocks)) + '/' + str("{:,}".format(total_blocks)) + ' (' + str(int(current_blocks*100/total_blocks)) + '%) Blocks processed')
                    #update to run and strip ls -l to get a more granular update
            return (total_blocks, current_blocks)
        
        
        def run_process(fexp, progress):
            print(fexp)
            
            # Start the subprocess.
            current_blocks = 0
            total_blocks = 0
            out_r, out_w = pty.openpty()
            err_r, err_w = pty.openpty()
            proc = subprocess.Popen([fexp], shell=True, stdout=out_w, stderr=err_w)
            os.close(out_w) # if we do not write to process, close these.
            os.close(err_w)
            if progress == True:
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
                        for line in lines:
                            total_blocks, current_blocks = check_line(line, total_blocks, current_blocks)
                            #print(line)
                        if not readable:
                            fds.remove(f)
            return
        
        import subprocess
        for f in fexp_scripts:
            print(f"Calling Fast Export on file...  {f}")
            if step_detail == True:
                run_process(f"fexp < {f}", True)
            else:
                run_process(f"fexp < {f}",False)
        #Parrelel execution needs to be further worked out. Not behaving as expected so we will come back to it later
        """
        if os.name == "nt":
            import subprocess
            for f in fexp_scripts:
                print(f"Calling Fast Export on file...  {f}")
                subprocess.call(f"fexp < {f}", shell=True)
        else:
            r = Parallel(n_jobs=-1, verbose=5)(delayed(call_sub)(f) for f in fexp_scripts)
        """

        data_file = f"{abs_path}/data/{table_name}_export.txt"
        #print("before did partition check")
        #print(did_partition)
        #print(fexp_scripts)
        #print(col_list)
        if did_partition:
            #First checking the case of vertical parrelelization

            if not isinstance(col_list[0], list):
                combine_type = "vertical"
            else:
                combine_type = "horizontal"
            #print(combine_type)
            concat_str, data_files, remove_cmd = combine_partitioned_file(fexp_scripts,combine_type=combine_type)


            #Concat and delete partition files
            #If we are doing a vertical concat, we use this
            #print("Concat str: " + str(concat_str))
            if concat_str:
                #print("In here")
                concat_files(concat_str)
            else:
                #If we are doing a horizontal concat we will read into memory and combine
                #This is because windows does not have a cood command in command prompt to do this operation as opposed to linux paste command
                _df = concat_files_horizontal(data_file, data_files, col_list, primary_keys, dtype_dict)
                col_list = _df.columns.tolist()



            for f in data_files:
                remove_file(remove_cmd, f)


        #raw_tbl_name = data_file.split("/")[-1].split(".")[0]
        if clean_and_serialize != False:
            if clean_and_serialize not in ["feather","pickle"]:
                raise Exception("Serialize must be either 'feather' or 'pickle'")
            print("Reading Table into memory...")
            #Need low_memory flag or else with large datasets we will end up with mixed datatypes
            if concat_str or did_partition == False:
                #If we have a concat_str, that means we need to read _df into memory for the first time
                #If it's false, that means that we already have it in memory from doing a horizontal combining
                _df = pd.DataFrame()
                try:
                    _df = pd.read_csv(data_file, names=col_list, sep="|", dtype=dtype_dict, na_values=["?","","~","!","null"])
                except Exception as e:
                    pass
                if len(_df) == 0:
                    _df = pd.read_csv(data_file, names=col_list, sep="|", dtype=dtype_dict, na_values=["?","","~","!","null"], encoding='latin1')

            print("Cleaning data...")
            for col in _df.columns.tolist():
                if _df[col].dtype == "object":
                    #_df[col] = _df[col].apply(lambda x: x.str.strip())
                    try:
                        _df[col] = _df[col].apply(lambda x: np.nan if pd.isnull(x) else x.strip())
                    except:
                        pass

            #Try to find date looking columns and cast them appropriately (We know the format of the date because we are explicit about it in the fastexport script)
            #Try to find id columns and convert to strings proactively
            for col in _df.columns.tolist():
                if clean_and_serialize != "feather":
                    if "_dt" in col:
                        try:
                            _df[col] =  pd.to_datetime(_df[col], format='%Y-%m-%d')
                        except:
                            pass
                if (("_id" in col) or ("_key" in col) or ("_cd" in col)):
                    try:
                        #print("before force string")
                        force_string(_df,col)
                        #print("after force string")
                    except:
                        pass

            print("Getting rid of any NaN exclusive columns")
            to_drop = []
            for col in _df.columns.tolist():
                if _df[col].isnull().all():
                    to_drop.append(col)
            _df.drop(to_drop, axis=1, inplace=True)

            print("Serializing data....")
            if clean_and_serialize == "feather":
                _df.to_feather(f"{abs_path}/serialized/{table_name}.feather")
                print("Finished: Your data file is located at:")
                print(f"{abs_path}/serialized/{table_name}.feather")
            elif clean_and_serialize == "pickle":
                _df.to_pickle(f"{abs_path}/serialized/{table_name}.pkl")
                print("Finished: Your data file is located at:")
                print(f"{abs_path}/serialized/{table_name}.pkl")

            t2 = time.time()
            m, s = divmod(int(t2-t1), 60)
            h, m = divmod(m, 60)
            print(f"Process took: {h} hours {m} minutes {s} seconds")
            #If you pickle the data, then you will have the column metadata already
            return
        else:
            print("Finished: Your end data file is located at:")
            print(data_file)
            print("You have chosen to not clean or serialize your data and fast export does not support export column names. \
                    Be sure to gather and keep in order these column names.")


        t2 = time.time()
        m, s = divmod(int(t2-t1), 60)
        h, m = divmod(m, 60)
        print(f"Process took: {h} hours {m} minutes {s} seconds")
    except Exception as e:
        print(f"Error: {e}")
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return

    return(col_list)
