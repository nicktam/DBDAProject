import pyspark
from pyspark.sql.types import *
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import split, explode
import pyspark.sql.functions as F
from pyspark.sql import SQLContext

sc = SparkContext('local')
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

##1 links
schema = StructType([StructField('movieid', IntegerType(), True),StructField('imdbid',  IntegerType(), True),StructField('tmdbid', IntegerType(), True)])
link = spark.read.csv("s3://projectdata26012020/ml-latest/links.csv", schema=schema , header=True)
link.write.format("orc").save("s3a://projectdata26012020/orc_files/links")

##2 movies
import re
def insertYear(title):
  year = re.search('\(([0-9]{4})', title)
  if year is not None:
    year = year.group(1)
  else:
    year = 0
  return int(year)

schema = StructType([StructField('movieid', IntegerType(), True),StructField('title', StringType(), True),StructField('genres', StringType(), True)])
movies = spark.read.csv("s3://projectdata26012020/ml-latest/movies.csv", schema=schema , header=True)
movies = movies.withColumn("genres", explode(split("genres", "[|]")))

sqlContext.udf.register("yearCleansing", insertYear)

year_udf = udf(insertYear, IntegerType())

df_movie_updated = movies.select("movieid","title","genres",year_udf("title").alias("year"))

df_movie_updated.write.format("orc").save("s3a://projectdata26012020/orc_files/movies/")

##3 gtags
schema = StructType([StructField('tagId', IntegerType(), True),StructField('tag', StringType(), True)])
genome_tags = spark.read.csv("s3://projectdata26012020/ml-latest/genome-tags.csv", schema=schema , header=True)
genome_tags.write.format("orc").save("s3a://projectdata26012020/orc_files/genome_tags/")

##4 gscores
schema = StructType([StructField('movieid', IntegerType(), True),StructField('tagId',  IntegerType(), True),StructField('relevance', FloatType(), True)])
genome_scores = spark.read.csv("s3://projectdata26012020/ml-latest/genome-scores.csv", schema=schema , header=True)
genome_scores = genome_scores.withColumn("relevance", func.round(genome_scores["relevance"], 3))
genome_scores.write.format("orc").save("s3a://projectdata26012020/orc_files/genome_scores/")

##5 tags
schema1 = (StructType().add("userid", IntegerType()).add("movieid", IntegerType()).add("tag", StringType()).add("timestamp", LongType()))
tags = spark.read.csv("s3://projectdata26012020/ml-latest/tags.csv", schema=schema1, header=True)
tag= tags.withColumn('Datetime', func.from_unixtime('timestamp').cast(TimestampType()))
tagclean = F.split(tag['Datetime'], ' ')
tag = tag.withColumn('date', tagclean.getItem(0))
tag= tag.withColumn('time', tagclean.getItem(1))
tag_clean = tag.drop('timestamp','Datetime')
tag_clean.write.format("orc").save("s3a://projectdata26012020/orc_files/tags_cleaned/")

##6 ratings
schema2 = (StructType().add("userid", IntegerType()).add("movieid", IntegerType()).add("rating", FloatType()).add("timestamp", LongType()))
ratings = spark.read.csv("s3://projectdata26012020/ml-latest/ratings.csv", schema=schema2, header=True)
import pyspark.sql.functions as func
ratings = ratings.withColumn("rating", func.round(ratings["rating"], 1))
from pyspark.sql import functions as func 
ratings= ratings.withColumn('Datetime', func.from_unixtime('timestamp').cast(TimestampType()))
ratings_split = F.split(ratings['Datetime'], ' ')
ratings = ratings.withColumn('date', ratings_split.getItem(0))
ratings = ratings.withColumn('time', ratings_split.getItem(1))
ratings = ratings.drop('timestamp','datetime')
ratings.write.format("orc").save("s3a://projectdata26012020/orc_files/ratings/")