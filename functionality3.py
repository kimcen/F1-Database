from pyspark.sql.functions import col
from pyspark.sql import SparkSession

# Returns standings for a specific race
def functionality3(spark, postgres_properties):
    # Load the necessary tables from PostgreSQL
    races_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="races", properties=postgres_properties)
    results_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="results", properties=postgres_properties)
    drivers_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="drivers", properties=postgres_properties)
    
    # Get the race name and year from the user
    race_name = input("Enter the race name: ").strip()
    if not race_name:
        print("Error: Race name cannot be empty.")
        return
    
    try:
        race_year = int(input("Enter the race year: "))
    except ValueError:
        print("Error: Invalid year. Please enter a valid integer.")
        return

    # Rename the 'name' column in the races_df to avoid conflict
    races_df = races_df.withColumnRenamed("time", "race_time")

    # Join the tables and filter for the specified race
    race_results_df = (races_df.join(results_df, "raceId")
                       .join(drivers_df, "driverId")
                       .filter((col("name") == race_name) & (col("year") == race_year))
                       .select("position", "forename", "surname", "positionText", "points", "laps", "time")
                       .orderBy(col("position").asc_nulls_last()))
    
    # Show the result
    print(f"\nResults for {race_name} {race_year}:")
    race_results_df.show(truncate=False)
    
    return race_results_df