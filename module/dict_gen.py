""" Module hosting the code for generation of dictionaries for substance normalisation mapping"""
import gzip
import json
import pickle
import re
from multiprocessing import Manager, Process
from time import perf_counter
from typing import Callable, List, Literal, Optional, Tuple

import pandas as pd

from .algo_conv import Hashdict

### CONSTANTS ###

TAG_REGEX = re.compile(r"</?[a-zA-Z0-9\"',\=-]+/?>")
HEAD = gzip.compress(bytes("{", "utf-8"), compresslevel=9)
MOD_TAIL = gzip.compress(bytes('".": "."}', "utf-8"), compresslevel=9)


### FUNCTIONS ###

## Main functions

def generate_dict(parquet_batchlist: List[List[str]], dict_output_path: str, index_output_path: str, pre_fn: Callable, mode: Literal['json', 'pickle']) -> None:
    """
    Function used for generating a gzipped dictionary from parquet files (for substance normalisation)
    """

    if mode == "json":
        storage_bytelist = [HEAD]
    elif mode == "pickle":
        storage_bytelist = []
    else:
        print("Mode chosen wrongly.")
        return

    ### main loop ###
    for i, parquet_batch in enumerate(parquet_batchlist):
        with Manager() as manager:
            ## from parquet to dict
            t1 = perf_counter()
            results = manager.list()
            procs = []
            for j, filepath in enumerate(parquet_batch):
                proc = Process(target=_parquet_to_dict, args=(j, filepath, results, pre_fn, mode))
                proc.daemon = True
                procs.append(proc)
                proc.start()

            for proc in procs:
                proc.join()
                
            t2 = perf_counter()
            time_taken = t2-t1
            print(f"Time taken to create strdict_list {i}: {time_taken}")
            
            t3 = perf_counter()
            storage_bytelist.extend(results)
            t4 = perf_counter()
            time_taken = t4-t3
            print(f"Time taken to extend to storage_bytelist: {time_taken}")
            
    if mode == "json":
        storage_bytelist.append(MOD_TAIL)

    ### writing section ###
    t1 = perf_counter()
    chunks_count = len(storage_bytelist)

    # writing the index chunk json (non-gzipped)
    index_list = _get_indices(storage_bytelist)
    with open(index_output_path, "w") as f:
        json.dump(index_list, f)

    # writing the gzipped file
    with open(dict_output_path, 'wb') as f:
        for i, compressed_chunk in enumerate(storage_bytelist):
            print(f"Writing chunk {i} out of {chunks_count}")
            f.write(compressed_chunk)
    t2 = perf_counter()
    time_taken = t2-t1
    print(f"Time taken to write final gzip: {time_taken}")

def _parquet_to_dict(i: int, filepath: str, results: list, pre_fn: Callable, mode: Literal['json', 'pickle']) -> None:
    """
    Process target function for converting parquet to dict
    """
    print(f"Started process {i}, filepath {filepath}")
    output_dict = {}
    df = pd.read_parquet(filepath)
    for _, row in df.iterrows():
        ## preprocess for substance_names
        # key = remove_tags(row[0])
        key = pre_fn(row[0])
        value = row[1]
        output_dict[key] = list(value)

    if mode == "json":
        results.append(gzip.compress(bytes(json.dumps(output_dict)[1:-1]+",",'utf-8'),compresslevel=9))
    elif mode == "pickle":
        results.append(gzip.compress(pickle.dumps(output_dict),compresslevel=9))
    
    print(f"Finished process {i}")

def _get_indices(bytelist: List[bytes]) -> List[Tuple[int,int]]:
    """
    Returns a list of start and end indices (cumsum style)
    """
    len_list = [len(x) for x in bytelist]
    index_list = []
    start = 0
    for x in len_list:
        stop = start + x
        index_list.append((start,stop))
        start = stop

    return index_list

## Pre-functions
def pass_fn(input: object) -> object:
    return input

def remove_tags(input_text: str) -> Optional[str]:
    '''
    Remove the html tags for single string
    '''
    if input_text:
        return re.sub(TAG_REGEX, "", input_text)
    else:
        return None
    
def to_hashdict(input_tuplist) -> Hashdict:
    output_dict = {}
    for tup in input_tuplist:
        try:
            ele, upper, lower = tup
            output_dict[ele] = (lower, upper)
        except:
            continue
    return Hashdict(output_dict)
