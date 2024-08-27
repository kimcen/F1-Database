from pyspark.sql.functions import col, avg, first, last, desc, round as spark_round
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

# Calculates the improvement for each driver by comparing their first and last qualifying positions of the season.
# Creates two bar charts:
# One showing the average qualifying position for each driver
# Another showing the average qualifying position for each constructor
def functionality6(spark, postgres_properties):
    try:
        season_year = int(input("Enter the season year: "))
    except ValueError:
        print("Error: Invalid year. Please enter a valid integer.")
        return

    races_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="races", properties=postgres_properties)
    qualifying_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="qualifying", properties=postgres_properties)
    drivers_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="drivers", properties=postgres_properties)
    constructors_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="constructors", properties=postgres_properties)

    # Rename the 'name' column in the races_df to avoid conflict
    races_df = races_df.withColumnRenamed("name", "race_name")

    season_data = (races_df.filter(col("year") == season_year)
                   .join(qualifying_df, "raceId")
                   .join(drivers_df, "driverId")
                   .join(constructors_df, "constructorId")
                   .select("raceId", "driverId", "forename", "surname", "position", "round", "name"))

    if season_data.count() == 0:
        print(f"No qualifying data found for the {season_year} season.")
        return

    # Calculate average qualifying position for each driver
    avg_positions = (season_data.groupBy("driverId", "forename", "surname")
                     .agg(spark_round(avg("position"), 2).alias("avg_position"))
                     .orderBy("avg_position"))

    # Calculate average qualifying position for each constructor
    avg_constructor_positions = (season_data.groupBy("name")
                                 .agg(spark_round(avg("position"), 2).alias("avg_position"))
                                 .orderBy("avg_position"))

    # Visualize average qualifying positions by driver
    plt.figure(figsize=(12, 6))
    driver_data = avg_positions.toPandas()
    plt.barh(driver_data['forename'] + driver_data['surname'], driver_data['avg_position'])
    plt.title(f"Average Qualifying Positions by Driver in {season_year} Season")
    plt.xlabel("Average Qualifying Position")
    plt.ylabel("Driver")
    plt.gca().invert_yaxis()  # To show best performers at the top
    plt.tight_layout()
    plt.savefig(f'/output/avg_qualifying_positions_drivers_{season_year}.png')
    print(f"Driver graph saved as 'avg_qualifying_positions_drivers_{season_year}.png' in the output directory.")

    # Visualize average qualifying positions by constructor
    plt.figure(figsize=(12, 6))
    constructor_data = avg_constructor_positions.toPandas()
    plt.barh(constructor_data['name'], constructor_data['avg_position'])
    plt.title(f"Average Qualifying Positions by Constructor in {season_year} Season")
    plt.xlabel("Average Qualifying Position")
    plt.ylabel("Constructor")
    plt.gca().invert_yaxis()  # To show best performers at the top
    plt.tight_layout()
    plt.savefig(f'/output/avg_qualifying_positions_constructors_{season_year}.png')
    print(f"Constructor graph saved as 'avg_qualifying_positions_constructors_{season_year}.png' in the output directory.")

    # Show average qualifying positions
    print("\nAverage Qualifying Positions by Driver:")
    avg_positions.show(truncate=False)

    print("\nAverage Qualifying Positions by Constructor:")
    avg_constructor_positions.show(truncate=False)

    # Identify drivers who improved the most throughout the season
    window_spec = Window.partitionBy("driverId").orderBy("round")
    driver_improvement = (season_data
                          .withColumn("first_qual", first("position").over(window_spec))
                          .withColumn("last_qual", last("position").over(window_spec))
                          .groupBy("driverId", "forename", "surname")
                          .agg(
                              first("first_qual").alias("first_race_position"),
                              first("last_qual").alias("last_race_position")
                          )
                          .withColumn("improvement", col("first_race_position") - col("last_race_position"))
                          .orderBy(desc("improvement")))

    print("\nDrivers who improved the most in qualifying:")
    driver_improvement.show(10, truncate=False)

    return season_data