from pyspark.sql.functions import sum, col, count, avg, desc, first, last, max, year, round as spark_round
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
import pandas
import time
import sys

def setup_database():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="hunter2"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Check if database already exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname='f1db'")
        exists = cursor.fetchone()
        if not exists:
            # Create a new database
            cursor.execute("CREATE DATABASE f1db")
        
        # Connect to the new database
        conn.close()
        conn = psycopg2.connect(
            host="localhost",
            database="f1db",
            user="postgres",
            password="hunter2"
        )
        cursor = conn.cursor()
        
        # Execute the SQL file
        with open('/data/f1db_postgre1.sql', 'r') as f1:
            cursor.execute(f1.read())
        with open('/data/f1db_postgre2.sql', 'r') as f2:
            cursor.execute(f2.read())

        conn.commit()
        conn.close()
        print("Database setup completed successfully.")
    except psycopg2.Error as e:
        print(f"PostgreSQL Error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error setting up database: {str(e)}")
        raise

def setup_spark():
    try:
        spark = SparkSession.builder \
            .appName("PySpark PostgreSQL Example") \
            .config("spark.jars", "/postgresql-42.6.0.jar") \
            .getOrCreate()
        return spark
    except Exception as e:
        print(f"Error setting up Spark: {str(e)}")
        sys.exit(1)

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

# Returns the number or races per season and saves it as sa line plot
def functionality2(spark, postgres_properties):
    # Load the races table from PostgreSQL
    races_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="races", properties=postgres_properties)
    
    # Group by year and count the number of races
    races_per_year = (races_df
                      .groupBy("year")
                      .agg(count("*").alias("race_count"))
                      .orderBy("year"))
    
    # Show the result
    print("\nEvolution of the number of races per season:")
    races_per_year.show(50, truncate=False)  # Showing up to 50 rows to cover all years
    
    # Create a line plot to visualize the trend
    # Convert to Pandas for plotting
    pandas_df = races_per_year.toPandas()
    
    plt.figure(figsize=(12, 6))
    plt.plot(pandas_df['year'], pandas_df['race_count'], marker='o')
    plt.title('Evolution of the Number of Races per Season')
    plt.xlabel('Year')
    plt.ylabel('Number of Races')
    plt.grid(True)
    plt.savefig('/output/races_per_season.png')
    print("Graph saved as 'races_per_season.png' in the /output directory.")

    return races_per_year

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

# SEASON DOMINANCE:
# Returns the seasons that had the most dominant constructors, 
# For each season, calculate what percentage of total points the winning constructor achieved, 
# and a plot of the most dominant seasons in F1 history
def functionality4(spark, postgres_properties):
    # Load necessary tables
    races_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="races", properties=postgres_properties)
    results_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="results", properties=postgres_properties)
    constructors_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="constructors", properties=postgres_properties)

    # Rename the 'name' column in the races_df to avoid conflict
    races_df = races_df.withColumnRenamed("name", "race_name")

    # Join tables and calculate points per constructor per season
    season_points = (races_df.join(results_df, "raceId")
                     .join(constructors_df, "constructorId")
                     .groupBy("year", "constructorId", "name")
                     .agg(sum("points").alias("total_points")))

    # Calculate total points per season and winning constructor's points
    window_spec = Window.partitionBy("year")
    dominant_seasons = (season_points
                        .withColumn("season_total_points", sum("total_points").over(window_spec))
                        .withColumn("max_points", max("total_points").over(window_spec))
                        .filter(col("total_points") == col("max_points"))
                        .withColumn("dominance_percentage", (col("max_points") / col("season_total_points")) * 100)
                        .select("year", "name", "total_points", "season_total_points", "dominance_percentage")
                        .orderBy(desc("dominance_percentage")))

    # Show the top 10 most dominant seasons
    print("Top 10 Most Dominant Seasons in F1 History:")
    dominant_seasons.show(10, truncate=False)

    # Convert to Pandas for plotting
    pandas_df = dominant_seasons.toPandas()

    # Create a bar plot of the top 15 most dominant seasons
    plt.figure(figsize=(15, 8))
    top_15 = pandas_df.head(15)
    plt.bar(top_15['year'].astype(str) + ' (' + top_15['name'] + ')', top_15['dominance_percentage'])
    plt.title('Top 15 Most Dominant Seasons in F1 History', fontsize=16)
    plt.xlabel('Season (Constructor)', fontsize=12)
    plt.ylabel('Percentage of Total Points', fontsize=12)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    # Save the plot
    plt.savefig('/output/dominant_seasons_f1.png', dpi=300, bbox_inches='tight')
    print("Graph saved as 'dominant_seasons_f1.png' in the output directory.")

    return dominant_seasons


# Plot the points of a pilot along the years
def functionality5(spark, postgres_properties):
    # Load the necessary tables from PostgreSQL
    races_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="races", properties=postgres_properties)
    results_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="results", properties=postgres_properties)
    drivers_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="drivers", properties=postgres_properties)

    # Get the driver name from the user
    driver_name = input("Enter the driver's full name (e.g., 'Lewis Hamilton'): ").strip()
    if not driver_name or len(driver_name.split()) < 2:
        print("Error: Please enter a valid full name (first name and last name).")
        return
    
    # Split the name into forename and surname
    forename, surname = driver_name.split(' ', 1)

    # Join the tables and calculate the total points per season for the specific driver
    driver_points = (races_df.join(results_df, "raceId")
                     .join(drivers_df, "driverId")
                     .filter((col("forename") == forename) & (col("surname") == surname))
                     .groupBy(col("year"))
                     .agg(sum(col("points").cast("float")).alias("total_points"))
                     .orderBy("year"))

    # Convert to Pandas for easier plotting
    pandas_df = driver_points.toPandas()

    # Create a line plot
    plt.figure(figsize=(12, 6))
    plt.plot(pandas_df['year'], pandas_df['total_points'], marker='o')
    plt.title(f'Points Evolution for {driver_name} Over Seasons')
    plt.xlabel('Year')
    plt.ylabel('Total Points')
    plt.grid(True)

    # Save the plot
    plt.savefig(f'/output/{driver_name.replace(" ", "_")}_points_evolution.png')
    print(f"Graph saved as '{driver_name.replace(' ', '_')}_points_evolution.png' in the output directory on your local machine.")

    # Show some statistics
    print("\nPoints per Season:")
    print(pandas_df.sort_values('total_points', ascending=False).to_string(index=False))

    best_season = pandas_df.loc[pandas_df['total_points'].idxmax()]
    print(f"\nBest Season: {best_season['year']} with {best_season['total_points']} points")

    return driver_points

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


def main():
    try:
        # Setup and populate the database
        setup_database()
        
        # Setup PySpark
        spark = setup_spark()
        
        # Define PostgreSQL connection properties
        postgres_properties = {
            "user": "postgres",
            "password": "hunter2",
            "driver": "org.postgresql.Driver"
        }
        
        # Basic input for testing
        while True:
            try:
                functionality = int(input("\nEnter the functionality number (1-6), or 0 to exit: "))
                if functionality == 0:
                    print("Exiting the program.")
                    break
                elif functionality not in range(1, 7):
                    print("Invalid functionality number. Please enter a number between 1 and 6.")
                    continue

                if functionality == 1:
                    functionality1(spark, postgres_properties)
                elif functionality == 2:
                    functionality2(spark, postgres_properties)
                elif functionality == 3:
                    functionality3(spark, postgres_properties)
                elif functionality == 4:
                    functionality4(spark, postgres_properties)
                elif functionality == 5:
                    functionality5(spark, postgres_properties)
                elif functionality == 6:
                    functionality6(spark, postgres_properties)
            except ValueError:
                print("Invalid input. Please enter a number.")
            except Exception as e:
                print(f"An error occurred: {str(e)}")

    except Exception as e:
        print(f"A critical error occurred: {str(e)}")
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()