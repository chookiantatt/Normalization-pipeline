""" Utility module for substance normalisation, to be imported into main script"""
import gzip
import json
import os
import pickle
from multiprocessing import Pool
from typing import List
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.types import (ArrayType, IntegerType, StringType, StructField,
                               StructType)

from .algo_conv import Hashdict

DF = pd.DataFrame

# Unmute for original parsing(subs_norm task)

## Defining the pyarrow schema for input
infer_struct = pa.struct([
    pa.field("start_orig", pa.int32()),
    pa.field("end_orig", pa.int32()),
    pa.field("word_orig", pa.string()),
])
infer_list = pa.list_(infer_struct)

INFER_SCHEMA_PA = pa.schema([
    pa.field('patent_id', pa.string()),
    pa.field("infer_list", infer_list)
])

## Defining the pyspark schema for output
output_struct = StructType([
    StructField("start_orig", IntegerType(), False),
    StructField("end_orig", IntegerType(), False),
    StructField("word_orig", StringType(), False),
    StructField("subs_id", StringType(), False),
    # StructField("section", StringType(), False)
])
output_list = ArrayType(output_struct)

OUTPUT_SCHEMA_PYSPARK = StructType([
    StructField("patent_id", StringType(), False),
    StructField("infer_list", output_list, False)
])

# Setup PySpark
def get_spark(role_arn: str, driver_memory: str, local_dir: str, app_name: str="pyspark_notebook", ):
    spark = SparkSession \
        .builder \
        .appName(app_name) \
        .config("spark.storage.memoryFraction", "0.0") \
        .config("spark.driver.maxResultSize", "0") \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.rdd.compress", "true") \
        .config("spark.hadoop.fs.s3a.fast.upload", "true") \
        .config("spark.hadoop.fs.s3a.connection.maximum", 100) \
        .config("spark.local.dir", local_dir) \
        .getOrCreate()

    return spark

# Functions
def run_aws_command(mode: str, from_path: str, to_path: str = None) -> None:
    """
    Function for generating the command-line commands in string form
    Which will be run via os.system

    :params mode: String for different actions
    :params from_path: String for the path to execute from
    :params to_path: String for the path to execute to, or to remove
    """
    # defining the string components
    rm = "aws s3 rm "
    cp = "aws s3 cp "
    rm_back = " --recursive"
    cp_back = " --recursive --exclude '*.crc' --exclude '*.txt'"
    sync = "aws s3 sync "
    sync_back = " --delete --exclude '*.crc' --exclude '*.txt'"

    # creating the command
    if mode == "remove":
        command_string = rm + to_path
    elif mode == "remove_dir":
        command_string = rm + to_path + rm_back
    elif mode == "copy":
        command_string = cp + from_path + " " + to_path
    elif mode == "copy_dir":
        command_string = cp + from_path + " " + to_path + cp_back
    elif mode == "sync_dir":
        command_string = sync + from_path + " " + to_path + sync_back
    else:
        print(
            "mode was chosen wrongly. Only 'remove', 'remove_dir', 'copy', 'copy_dir', and 'sync_dir' are allowed."
        )
        return  # to prevent running of any strange commands

    # running the command
    os.system(command_string)

def batch_pathlist(pathlist: List[str], batch_size: int) -> List[List[str]]:
    """
    Returns a list of lists for batching purposes
    """
    return [pathlist[i:i+batch_size] for i in range(0, len(pathlist), batch_size)]


def load_parquet_pandas(pathlist: List[str], target_path: str, spark_instance: object) -> DF:
    """
    Loads a list of parquet parts via their paths and
    Returns a concatenated single Pandas DF
    """
    parquet_path = [os.path.join(target_path, path) for path in pathlist]
    dataset = pq.ParquetDataset(parquet_path)
    table = dataset.read()
    return table.to_pandas()

def load_gzip_json(json_pathway: str) -> dict:
    """
    Loads from gzipped json file
    """
    with gzip.open(json_pathway, "r") as fin:
        jsonfile = json.loads(fin.read().decode("utf-8"))

    return jsonfile

def decompress_chunk_to_pkldict(comp_bytes: bytes) -> dict:
    return pickle.loads(gzip.decompress(comp_bytes))

def multiproc_load_gzip(dict_pathway: str, index_pathway: str, cpu_count: int) -> dict:
    """
    Decompresses json or pickle file using multiple cores
    Requires the chunking index json file as well for the exact locations of the gzip chunks
    """
    ## loading the chunking index file
    with open(index_pathway, 'r') as f:
        index_list = json.load(f)

    ## loading the actual dict
    with open(dict_pathway, 'rb') as f:
        full_chunk = f.read()

    chunk_list = [full_chunk[start:end] for start, end in index_list]

    with Pool(cpu_count) as p:
        processed_list = p.map(decompress_chunk_to_pkldict, chunk_list)

    del chunk_list

    for i in range(len(processed_list)-1, 0, -1):
        processed_list[0] |= processed_list[i]
        del processed_list[i]
    return processed_list[0]

