import os
import argparse
import yaml
import logging
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def parse_args():
    parser = argparse.ArgumentParser("Extract data")
    parser.add_argument('--config', type=str, required=True, help="Path to the config YAML")
    return parser.parse_args()


def main():
    logging.info("Starting data extraction")
    try:
        args = parse_args()
        with open(args.config, "r") as f:
            config = yaml.safe_load(f)

        input_dir = config["raw_data_dir"]
        output_dir = config["extract_data_dir"]

        # Initialize spark session
        spark = SparkSession.builder.appName("MovieLensIngestion").getOrCreate()
        
        # Load movies
        movie_df = spark.read.option("sep","::").csv(os.path.join(input_dir,"movies.dat")).toDF("movieID", "title", "genres")
        logging.info(f"Movies loaded: {movie_df.count()} rows")

        # Load ratings 
        rating_df = spark.read.option("sep","::").csv(os.path.join(input_dir,"ratings.dat")).toDF("userID","movieID","rating","timestamp")
        logging.info(f"Ratings loaded: {rating_df.count()} rows")
        
        # Load users
        user_df = spark.read.option("sep","::").csv(os.path.join(input_dir,"users.dat")).toDF("userID","gender","age","occupation","zipcode")
        logging.info(f"Users data loaded: {user_df.count()} rows")
        
        # Save as parquet
        movie_df.write.mode("overwrite").parquet(os.path.join(output_dir,"movies"))
        rating_df.write.mode("overwrite").parquet(os.path.join(output_dir,"ratings"))
        user_df.write.mode("overwrite").parquet(os.path.join(output_dir,"users"))
        
        # stop the spark session
        spark.stop()
        logging.info("Extraction completed.")
    except Exception as e:
        logging.error(f"Extraction failed: {str(e)}")

if __name__ == '__main__':
    main()