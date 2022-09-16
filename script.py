import pyspark
from pyspark.sql import SparkSession
import time


# reserve time for termination action
def suspend(secs, count):
    for i in range(count):
        print("Waiting" + "."*10)
        time.sleep(secs)


def calculate_red_violations(data_source, output_uri):
    """
    Processes sample food establishment inspection data and queries the data to find the top 10 establishments
    with the most Red violations from 2006 to 2020.

    :param data_source: The URI of your food establishment data CSV, such as 's3://DOC-EXAMPLE-BUCKET/food-establishment-data.csv'.
    :param output_uri: The URI where output is written, such as 's3://DOC-EXAMPLE-BUCKET/restaurant_violation_results'.
    """
    # init
    spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

    # Load the restaurant violation CSV data
    restaurants_df = spark.read.option("header", "true").csv(data_source)

    # Create an in-memory DataFrame to query
    restaurants_df.createOrReplaceTempView("restaurant_violations")

    # Create a DataFrame of the top 10 restaurants with the most Red violations
    top_red_violation_restaurants = spark.sql("""SELECT name, count(*) AS total_red_violations 
        FROM restaurant_violations 
        WHERE violation_type = 'RED' 
        GROUP BY name 
        ORDER BY total_red_violations DESC LIMIT 10""")

    # Write the results to the specified output URI
    top_red_violation_restaurants.write.option(
        "header", "true").mode("overwrite").csv(output_uri+'/first')
    suspend(5, 5)
    top_red_violation_restaurants.write.option(
        "header", "true").mode("overwrite").csv(output_uri+'/second')
    suspend(5, 5)
    top_red_violation_restaurants.write.option(
        "header", "true").mode("overwrite").csv(output_uri+'/third')


if __name__ == "__main__":
    calculate_red_violations(
        "s3://YOUR_BUCKET_NAME/data/food_establishment_data.csv", "s3://YOUR_BUCKET_NAME/result")
