""" Module containing the multiprocessing functions, to be imported into main script"""
# Import libraries
from multiprocessing import Lock, Manager, Process, Queue
from typing import Callable, List, Optional, Union

import pandas as pd
import pyspark

DF = pd.DataFrame

def multiproc_chain(input_data:Union[List[DF],DF], cpu_count, process_chain_dict: dict, split_df: bool, join_df: bool) -> Union[List[DF],DF]:
    """
    Multiprocessing normalisation of NER detected substance name, return result as pandas dataframe
    """
    if split_df:
        chunk_size = len(input_data) // cpu_count
        pdf_list = [input_data.loc[i:i+chunk_size].copy() for i in range(0, len(input_data), chunk_size)]
    else:
        pdf_list = input_data
    
    with Manager() as manager:
        lock = Lock()
        results = manager.Queue()
        procs = []
        for i, chunk in enumerate(pdf_list):
            proc = Process(target=_process_chain, args=(i, chunk, process_chain_dict, lock, results))
            proc.daemon = True
            procs.append(proc)
            proc.start()

        for proc in procs:
            proc.join()

        results_list = [results.get() for _ in range(len(procs))]

        if join_df:
            # Combine the results from all processes
            combined_result = pd.concat(results_list, ignore_index=True)

            print("Combined Result:")
            print(combined_result)
            return combined_result
        else:
            return results_list

def _add_key(input_dict: dict, input_key: str, input_value: str, section: str) -> dict:
    '''qq
    Adds a key with value to the dictionary
    Returns the edited dictionary
    '''
    if input_value is not None:
        input_dict[input_key] = input_value
        input_dict['section'] = section
    return input_dict

def tag_norm_list_set(input_dictlist_series: List[List[dict]], fn_args:tuple, pid: int) -> List[dict]:
    '''
    Combination of tagging and normalisation steps, but for a list of dictionaries as a whole
    Pulls out the target ("word_orig" in this case) into a list for processing
    For the entire pandas Series
    '''
    taglist_function, norm_dict, ebs_path, section = fn_args
    ## Pulling all word_orig
    input_strlist = [x["word_orig"] if not x.get("subs_id") else "" 
                     for input_dictlist in input_dictlist_series for x in input_dictlist]
    processed_list = taglist_function(input_strlist, pid, ebs_path)
    norm_list = [norm_dict.get(x, "") if x != "" else "" for x in processed_list]

    ## setting it back
    index_count = 0
    output_dictlist_series = []
    for input_dictlist in input_dictlist_series:
        print("Length of input_dictlist:", len(input_dictlist))
        print("Length of norm_list:", len(norm_list))
        output_dictlist = [_add_key(x, "subs_id", norm_list[index_count+i], section) for i, x in enumerate(input_dictlist) if len(norm_list)>0]
        index_count += len(input_dictlist)
        output_dictlist_series.append(output_dictlist)

    return output_dictlist_series


def tag_norm_set(tag_function: Callable, norm_dict: dict, input_dict: dict) -> Optional[dict]:
    '''
    Combination of tagging and normalisation steps
    For individual row (under an "apply" format)
    '''
    if type(input_dict) == pyspark.sql.types.Row:
        convert_dict = input_dict.asDict()
    else:
        convert_dict = input_dict.copy()
    output_field = tag_function(convert_dict["word_orig"])
    if output_field is not None:
        norm_field = norm_dict.get(output_field)
        if norm_field is not None:
            convert_dict["subs_id"] = norm_field
            return convert_dict
        else:
            convert_dict["subs_id"] = None
    else:
        convert_dict["subs_id"] = None
    return convert_dict
    

def filter_set(filter_function: Callable, input_dict: dict) -> Optional[dict]:
    '''
    For filtering the word_orig
    '''
    if filter_function(input_dict['word_orig']) is None:
        return None
    else:
        return input_dict
    
def drop_none(input_dictlist: List[dict]):
    """
    Drops all dicts in the list where subs_id key is missing
    """
    return [x for x in input_dictlist if x.get('subs_id')]

def drop_none_ppi(input_dictlist: List[dict]):
    """
    Drops all dicts in the list where ppi_id key is missing
    """
    return [x for x in input_dictlist if len(str(x.get('subs_id'))) == 8]

def drop_none_subsid(input_dictlist: List[dict]):
    """
    Drops all dicts in the list where ppi_id key is missing
    """
    return [x for x in input_dictlist if isinstance(x.get('subs_id'), list)]


def pass_function(input_str: str) -> str:
    return input_str

def dictlist_func(input_dictlist: List[dict], outer_fn: Callable, fn_args: tuple, mode: str) -> List[dict]:
    """
    Used for running a function onto dictionaries in a list
    """
    tag_fn, norm_dict = fn_args
    if mode == "apply":
        if len(input_dictlist) == 0:
            return []
        if type(input_dictlist[0]) == pyspark.sql.types.Row:
            conv_list = [x.asDict() for x in input_dictlist]
            output_list = [outer_fn(tag_fn, norm_dict, x) if not x.get('subs_id') else x for x in conv_list]
        else:
            output_list = [outer_fn(tag_fn, norm_dict, x) if not x.get('subs_id') else x for x in input_dictlist]
        return [x for x in output_list if x is not None]
    elif mode == "filter":
        return [x for x in input_dictlist if outer_fn(tag_fn, x) is None]

def _process_chain(pid: int, df: DF, process_dict: dict, lock: Lock, proc_chain_results: Queue) -> DF:
    '''
    Process chain encompassing all the processed, to be run by the multiproc
    '''
    ## unpacking the arguments
    for step_name, process_tuple in process_dict.items():
        print(f"Start of {step_name} for process {pid}:")
        fn, fn_args, mode = process_tuple
        if mode in ["apply", "filter"]:
            df['infer_list'] = df['infer_list'].apply(dictlist_func, args=(fn, fn_args, mode))
        elif mode == "group_run":
            series_dictlist = df['infer_list'].tolist()
            df['infer_list'] = tag_norm_list_set(series_dictlist, fn_args, pid)
        elif mode in ["drop", "select"]:
            df['infer_list'] = df['infer_list'].apply(fn)
        print(f"End of {step_name} for process {pid}")

    proc_chain_results.put(df)
        


