import gzip
import json
import logging
import os
import sys
from multiprocessing import Pool

import pyarrow as pa
import pyarrow.parquet as pq
from module.util_module import INFER_SCHEMA_PA, get_spark, run_aws_command

if len(sys.argv) >= 5:
    SPARK_DRIVER_MEMORY = sys.argv[1]
    GZIP_S3_PATH = sys.argv[2]
    CONVERTED_PARQUET_S3_PATH = sys.argv[3]
    EFS_PATH = sys.argv[4]
else:
    os._exit(1)

# Constant
ROLE_ARN = 'xxx'  # Input your aws role, it's better to store in some files then just read
SPARK_APP_NAME = "conversion_to_parquet"
CPU_COUNT = int(os.cpu_count()) - 1
PART_SIZE = 10000
EBS_PATH = "xxx"

# Path

## Local Path
NER_LOCAL_PATH = f"{EBS_PATH}/ner_gzip"
NER_PARQUET_LOCAL_PATH = f"{EBS_PATH}/ner_parquet"
NER_SUBS_PARQUET_LOCAL_PATH = f"{EBS_PATH}/ner_substance"

# Functions
def reformat_patent_inference(p: dict) -> dict:
    """
    To return selected field from raw input
    """
    return {
        "patent_id": p['patent_id'],
        "infer_list": [
            {
                "start_orig": infer['start_orig'],
                "end_orig": infer['end_orig'],
                # "word": infer['word'],
                "word_orig": infer['word_orig'],
                # "entity_group": infer['entity_group'],
            } for result in p['data']['results_list'] for infer in result['infer_list'] if infer['entity_group'] == "CEM"
        ]
    }

    

def convert_gzip_to_parquet(filepath: str) -> None:
    """
    Pool function for converting the gzip files into dictionary format 
    and then writing to parquet through pyarrow table
    """
    ## opening the file
    with gzip.open(filepath) as f:
        output_list = [reformat_patent_inference(json.loads(x.decode())) for x in f.readlines()]
        
    ## converting to table
    table = pa.Table.from_pylist(output_list, schema=INFER_SCHEMA_PA)
    
    ## figuring out the path
    name = filepath.split("/")[-1].split("_")[2]
    
    ## writing to parquet path
    with pq.ParquetWriter(f'{NER_PARQUET_LOCAL_PATH}/{name}.parquet', schema=INFER_SCHEMA_PA) as writer:
        writer.write_table(table)


if __name__ == '__main__':
    # Start logging
    logging.basicConfig(filename=f"{EFS_PATH}/logging.txt", level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Copy from S3 to local
    logging.info("Copying S3 raw data to local ebs... \n")
    run_aws_command("sync_dir", GZIP_S3_PATH, NER_LOCAL_PATH)

    # Create target list
    gzip_list = os.listdir(NER_LOCAL_PATH)
    os.makedirs(NER_PARQUET_LOCAL_PATH, exist_ok=True)
    ## To track incomplete parquet 
    splitted = [filename.split(".")[0] for filename in os.listdir(NER_PARQUET_LOCAL_PATH)]

    logging.info(f"Total completed parquet part: {len(splitted)} \n")

    non_complete_list = []
    # To store incomplete part into list
    for files in gzip_list:
        if any([filename in files for filename in splitted]):
            continue
        else:
            non_complete_list.append(files)

    logging.info(f"Incomplete part: {non_complete_list}  \n")

    gzip_list = [f"{NER_LOCAL_PATH}/{x}" for x in non_complete_list]
    logging.info(f"Total files to process: {len(gzip_list)}  \n")

    # Splitting into batches
    logging.info("Splitting gzip files to process into batches of CPU count")
    batch_list = [gzip_list[i:i+CPU_COUNT] for i in range(0, len(gzip_list), CPU_COUNT)]

    # Start multiprocessing gzip into parquet
    for i, batch in enumerate(batch_list):
        logging.info(f"Batch {i} \n")
        print(f"Batch {i} \n")
        with Pool(CPU_COUNT) as p:
            p.map(convert_gzip_to_parquet, batch)
        logging.info(f"Batch {i} done \n")
        print(f"Batch {i} done \n")

    # Initialize spark
    logging.info("Initializing spark... \n")
    spark = get_spark(ROLE_ARN, SPARK_DRIVER_MEMORY, f"{EBS_PATH}/tmp/", SPARK_APP_NAME)

    # Read into spark df and count
    print(f"Reading parquet... \n")
    df = spark.read.parquet(NER_PARQUET_LOCAL_PATH)
    df.printSchema()

    df_count = df.count()

    logging.info(f" Count: {df_count}  \n")

    # Write parquet in local directory
    logging.info("Writing parquet in local ebs... \n")
    df.write.parquet(NER_SUBS_PARQUET_LOCAL_PATH)

    # Copy parquet to S3
    logging.info("Copying result to S3 \n")
    run_aws_command("remove_dir", "", CONVERTED_PARQUET_S3_PATH)
    run_aws_command("sync_dir", NER_SUBS_PARQUET_LOCAL_PATH, CONVERTED_PARQUET_S3_PATH)

    logging.info("Finished copying... \n")