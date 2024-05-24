""" Main script for substance normalisation, importing functions and classes from all other scripts """
import os
import json
from time import perf_counter
from pyspark.sql import functions as F
from module.algo_conv import (CasNumberNormalizer, ChemFormNormalizer,
                               RubbishFilter)
from module.multiproc import (filter_set, multiproc_chain,
                              pass_function, tag_norm_list_set, tag_norm_set, drop_none_ppi, drop_none_subsid)
from module.opsin import str2opsin
from module.util_module import (OUTPUT_SCHEMA_PYSPARK, batch_pathlist,
                                get_spark, load_parquet_pandas,
                                multiproc_load_gzip, run_aws_command)

SPARK_DRIVER_MEMORY = "300g"
TARGET_INPUT_PARQUET_PATH = "xxxx"
OUTPUT_ENTITY_PARQUET_PATH = "xxx"
OUTPUT_PARQUET_PATH = "xxx"
BATCH_SIZE = 45
SECTION = "desc"

SPARK_APP_NAME = "substance_norm"
ROLE_ARN = 'xxx'
CPU_COUNT = int(os.cpu_count())
OUTPUT_PARTITION = 200

# local paths for the data and dicts
SUBS_NAME_DICT_LOCAL_PATH = "xxx"
CAS_DICT_LOCAL_PATH = "xxx"
CHEM_FORM_DICT_LOCAL_PATH = "xxx"
OPSIN_DICT_LOCAL_PATH = "xxx"
SUBS_NAME_DICT_INDEX_LOCAL_PATH = "xxx"
CAS_DICT_INDEX_LOCAL_PATH = "xxx"
CHEM_FORM_DICT_INDEX_LOCAL_PATH = "xxx"
OPSIN_DICT_INDEX_LOCAL_PATH = "xxx"
ENTITY_DICT_LOCAL_PATH = "xxx"

parquet_list = [x for x in os.listdir(TARGET_INPUT_PARQUET_PATH) if x.endswith(".parquet")]

if __name__ == '__main__':
    ## define the spark instance
    spark = get_spark(ROLE_ARN, SPARK_DRIVER_MEMORY, "xxx", SPARK_APP_NAME)

    cnn = CasNumberNormalizer()
    cfn = ChemFormNormalizer()
    rf = RubbishFilter()

    pathlist_batchlist = batch_pathlist(parquet_list, BATCH_SIZE)

    for i, batch in enumerate(pathlist_batchlist):
        ## loading the parquet parts
        input_df = load_parquet_pandas(batch, TARGET_INPUT_PARQUET_PATH, spark)

        # first set
        # loading the dictionary
        t_load1 = perf_counter()
        print("Loading the substance name norm dict...\n")
        norm_dict = multiproc_load_gzip(SUBS_NAME_DICT_LOCAL_PATH, SUBS_NAME_DICT_INDEX_LOCAL_PATH, CPU_COUNT)
        t_loaded1 = perf_counter()
        print(f"Time taken to load substance name norm dict : {t_loaded1 - t_load1} \n")
        first_process_dict = {
            "subs_norm": (tag_norm_set, (pass_function, norm_dict), "apply")
        }
        t1 = perf_counter()
        input_df_list = multiproc_chain(input_df, CPU_COUNT, first_process_dict, True, False)
        t2 = perf_counter()
        print(f"Time taken for substance name normalisation : {t2 - t1}")

        # second set
        del norm_dict
        print("Loading the CAS norm dict...\n")
        t_load2 = perf_counter()
        norm_dict = multiproc_load_gzip(CAS_DICT_LOCAL_PATH, CAS_DICT_INDEX_LOCAL_PATH, CPU_COUNT)
        t_loaded2 = perf_counter()
        print(f"Time taken to load CAS norm dict : {t_loaded2 - t_load2} \n")
        second_process_dict = {
            "filter_rubbish": (filter_set, (rf.filter_rubbish, ""), "filter"),
            "CAS_norm": (tag_norm_set, (cnn.extract_cas_num, norm_dict), "apply"),
        }
        t3 = perf_counter()
        input_df_list = multiproc_chain(input_df_list, CPU_COUNT, second_process_dict, False, False)
        t4 = perf_counter()
        print(f"Time taken for CAS normalisation : {t4 - t3}")

        ## third set
        del norm_dict
        print("Loading the chem_formula norm dict...\n")
        t_load3 = perf_counter()
        norm_dict = multiproc_load_gzip(CHEM_FORM_DICT_LOCAL_PATH, CHEM_FORM_DICT_INDEX_LOCAL_PATH, CPU_COUNT)
        t_loaded3 = perf_counter()
        print(f"Time taken to load chem_formula norm dict : {t_loaded3 - t_load3} \n")
        third_process_dict = {
            "chem_form_norm": (tag_norm_set, (cfn.extract_substance_hashdict, norm_dict), "apply")
        }
        t5 = perf_counter()
        input_df_list = multiproc_chain(input_df_list, CPU_COUNT, third_process_dict, False, False)
        t6 = perf_counter()
        print(f"Time taken for chem_form normalisation : {t6 - t5}")

        ## fourth set
        del norm_dict
        print("Loading the opsin dict...\n")
        t_load4 = perf_counter()
        norm_dict = multiproc_load_gzip(OPSIN_DICT_LOCAL_PATH, OPSIN_DICT_INDEX_LOCAL_PATH, CPU_COUNT)
        t_loaded4 = perf_counter()
        print(f"Time taken to load opsin norm dict : {t_loaded4 - t_load4} \n")
        fourth_process_dict = {
            "opsin_norm": (tag_norm_list_set, (str2opsin, norm_dict, "/home/jovyan/work/kiantatt/tmp", SECTION), "group_run"),
        }

        t7 = perf_counter()
        input_df_list = multiproc_chain(input_df_list, CPU_COUNT, fourth_process_dict, False, False)
        t8 = perf_counter()
        print(f"Time taken for opsin normalisation : {t8 - t7}")

        del norm_dict
        # last dict
        print("Loading entity dict...\n")
        with open(ENTITY_DICT_LOCAL_PATH, 'r') as json_file:
            norm_dict = json.load(json_file)
        fifth_process_dict = {
            "entity_norm": (tag_norm_set, (pass_function, norm_dict), "apply")
        }
        t11 = perf_counter()
        input_df_list = multiproc_chain(input_df_list, CPU_COUNT, fifth_process_dict, False, False)
        t12 = perf_counter()
        print(f"Time taken for entity normalisation : {t12 - t11}")

        del norm_dict
        ## final set
        final_process_dict = {
            "drop_none_ppi": (drop_none_ppi, ("",""), "drop")
        }
        t13 = perf_counter()
        output_ppi_df = multiproc_chain(input_df_list, CPU_COUNT, final_process_dict, False, True)
        t14 = perf_counter()
        print(f"Time taken for drop_none : {t14 - t13}")

        final_process_dict = {
            "drop_none_subsid": (drop_none_subsid, ("",""), "drop")
        }
        t13 = perf_counter()
        output_subs_df = multiproc_chain(input_df_list, CPU_COUNT, final_process_dict, False, True)
        t14 = perf_counter()
        print(f"Time taken for drop_none : {t14 - t13}") 

        ## converting back to spark dataframe, bucketed by patent_id (for better joining later)
        output_subs_spark_df = spark.createDataFrame(output_subs_df, schema=OUTPUT_SCHEMA_PYSPARK)
        output_ppi_spark_df = spark.createDataFrame(output_ppi_df, schema=OUTPUT_SCHEMA_PYSPARK)
 
        rev_ppi_df = output_ppi_spark_df.withColumn(
            "infer_list",
            F.col("infer_list").cast("array<struct<start_orig: int, end_orig: int, word_orig: string, ppi_id: string>>")
        )
        rev_ppi_df.repartition(OUTPUT_PARTITION, "patent_id").write.bucketBy(
            OUTPUT_PARTITION, "patent_id"
        ).sortBy("patent_id").option("path", f"{OUTPUT_ENTITY_PARQUET_PATH}_{i}").mode(
            "overwrite"
        ).saveAsTable(
            f"{SECTION}_bucketed_{i}", format="parquet"
        )

        ## subs_id output
        output_subs_spark_df.repartition(OUTPUT_PARTITION, "patent_id").write.bucketBy(
            OUTPUT_PARTITION, "patent_id"
        ).sortBy("patent_id").option("path", f"{OUTPUT_PARQUET_PATH}_{i}").mode(
            "overwrite"
        ).saveAsTable(
            f"{SECTION}_bucketed_{i}", format="parquet"
        )

