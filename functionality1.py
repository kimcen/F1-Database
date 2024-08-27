from pyspark.sql.functions import col, count
from pyspark.sql import SparkSession

# Driver who won a specific race the most times and the number of times they won
def functionality1(spark, postgres_properties):
    # Load the necessary tables from PostgreSQL
    races_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="races", properties=postgres_properties)
    results_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="results", properties=postgres_properties)
    drivers_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="drivers", properties=postgres_properties)
    
    # Get the race name from the user
    race_name = input("Enter the race name: ").strip()
    if not race_name:
        print("Error: Race name cannot be empty.")
        return
    
    # Join the tables and filter for the specified race and winners
    winners_df = (races_df.join(results_df, "raceId")
                  .join(drivers_df, "driverId")
                  .filter((col("name") == race_name) & (col("position") == 1))
                  .select("name", "forename", "surname"))
    
    # Count the wins for each driver
    win_counts = (winners_df.groupBy("forename", "surname")
                  .agg(count("*").alias("wins"))
                  .orderBy(col("wins").desc()))
    
    # Get the driver with the most wins
    top_winner = win_counts.limit(1)
    
    # Show the result
    top_winner.show()
    return
