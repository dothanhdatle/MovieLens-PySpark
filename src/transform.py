import os
import argparse
import yaml
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, regexp_replace, col, split, explode, from_unixtime, year, month, when

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def parse_args():
    parser = argparse.ArgumentParser("Transform data")
    parser.add_argument('--config', type=str, required=True, help="Path to the config YAML")
    return parser.parse_args()

def main():
    logging.info("Starting transforming data")
    try:
        args = parse_args()
        with open(args.config, "r") as f:
            config = yaml.safe_load(f)

        input_dir = config["extract_data_dir"]
        output_dir = config["transform_data_dir"]


        spark = SparkSession.builder.appName("Transform data").getOrCreate()
        
        # Load extracted data from parquet
        movie_df = spark.read.parquet(os.path.join(input_dir, "movies"))
        rating_df = spark.read.parquet(os.path.join(input_dir, "ratings"))
        user_df = spark.read.parquet(os.path.join(input_dir, "users"))

        # Cast data type 
        movie_df = movie_df.withColumn("movieID", col("movieID").cast("int"))

        rating_df = rating_df.withColumn("userID", col("userID").cast("int")) \
        .withColumn("movieID", col("movieID").cast("int")) \
        .withColumn("rating", col("rating").cast("float")) \
        .withColumn("timestamp", col("timestamp").cast("long"))

        user_df = user_df.withColumn("userID", col("userID").cast("int")) \
        .withColumn("age", col("age").cast("int")) \
        .withColumn("occupation", col("occupation").cast("int"))

        # Remove null values
        movie_df = movie_df.na.drop()
        rating_df = rating_df.na.drop()
        user_df = user_df.na.drop()

        # Remove duplicates
        movie_df = movie_df.dropDuplicates() 
        rating_df = rating_df.dropDuplicates()
        user_df = user_df.dropDuplicates()

        ## Transform the movies data
        # reformat the movie data by separating the title and year into 2 columns 
        movie_df = movie_df.withColumn("year", regexp_extract(col("title"),r"\((\d{4})\)",1).cast("int"))
        movie_df = movie_df.withColumn("title", regexp_replace(col("title"),r"\((\d{4})\)",""))
        movie_df.write.mode("overwrite").parquet(os.path.join(output_dir,"movies_clean"))


        # Movies by the genres
        movie_by_genre = movie_df.withColumn("genre", explode(split(col("genres"),"\|")))
        movie_by_genre = movie_by_genre.drop("genres")
        movie_by_genre.write.mode("overwrite").parquet(os.path.join(output_dir,"movies_by_genre"))

        ## Transform the rating data
        # Transform the timestamp to date time 
        ratingWithDatetime = rating_df.withColumn("datetime", from_unixtime("timestamp")) \
        .withColumn("year", year(from_unixtime("timestamp")))\
        .withColumn("month", month(from_unixtime("timestamp")))

        ratingWithDatetime.write.mode("overwrite").parquet(os.path.join(output_dir, "ratingWithDatetime"))

        ## Transform the users data
        # create column for age group description 
        user_df = user_df.withColumn("age_group", when(col("age") == 1, "Under 18")
        .when(col("age") == 18, "18-24")
        .when(col("age") == 25, "25-34")
        .when(col("age") == 35, "35-44")
        .when(col("age") == 45, "45-49")
        .when(col("age") == 50, "50-55")
        .when(col("age") == 56, "56+")
        )

        user_df.write.mode("overwrite").parquet(os.path.join(output_dir, "usersData"))

        # stop the spark session
        spark.stop()

        logging.info("Transformation completed.")
    except Exception as e:
        logging.error(f"Transform failed: {str(e)}")

if __name__ == '__main__':
    main()




