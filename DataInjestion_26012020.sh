#!/bin/bash

wget http://files.grouplens.org/datasets/movielens/ml-latest.zip
unzip ml-latest.zip
hdfs dfs -put ml-latest s3://projectdata26012020/

#hdfs dfs -get s3://projectgroup5/project_files/SparkStep24012020.py
#spark-submit --deploy-mode client --master yarn-client SparkStep24012020.py

#hdfs dfs -get s3://projectgroup5/project_files/HiveStep.hive
#hive-script --run-hive-script --args -f /home/hadoop/HiveStep.hive
