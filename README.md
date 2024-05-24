# Normalization-pipeline

# This is a chemical substance normalization project

# Overview
This project is targeted at downstream task from Named Entity Recognition output, where the substances are detected from journal, research papers or patents, to be normalized(assign an ID) with a huge substance database.

# Key process
This main script includes pre-processing steps, converting input gzip files from NER output into parquet format, then loads into pandas dataframe for main normalisation step.
The reason of not using PySpark dataframe is because the broadcast values of python dictionary will cause overhead in local machine's RAM, the scale of huge dictionary refers to >100m substance names tagged with a list of key indentifiers(ID).

# Customise dictionary
There are in total five dictionaries used in this project:
1. substance name
   
2. CAS(Chemical Abstract Society) number
   
3. chemical formula
   
4. opsin (module to convert IUPAC to InChIKey, refer opsin.py)
   
5. entity

There's a script for generating dictionaries for normalisation(dict_gen.py) into pickle format. 

# Other scripts:
algo_conv : to parse chemical formula written in HTML format

multiproc : multiprocessing for normalisation and dictionary loading steps

util_module: general functions including spark initialization, copying data from AWS S3 to local machines.

opsin: parse IUPAC to InChIKey

conversion_gzip_parquet: convert gzip files to parquet files
