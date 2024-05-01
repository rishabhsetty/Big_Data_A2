#!/bin/bash

input_path="A2Big_Data/q1" # in hdfs
output_path="q1_output" # in hdfs and must not exist
python_path=$(pwd)
hadoop_lib_path="/opt/hadoop/hadoop/share/hadoop/tools/lib"

yarn jar ${hadoop_lib_path}/hadoop-streaming-2.10.1.jar \
       -files ${python_path}/word_count_mapreduce.py,${python_path}/q1_reducer.py \
    -input ${input_path} \
    -output ${output_path} \
    -mapper word_count_mapreduce.py \
    -reducer q1_reducer.py
