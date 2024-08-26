import psycopg2
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import pandas
import time

def setup_database():
    # Wait for PostgreSQL to start
    time.sleep(5)
    
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
    except Exception as e:
        print(f"Error setting up database: {str(e)}")
        raise

def setup_spark():
    return SparkSession.builder \
        .appName("PySpark PostgreSQL Example") \
        .config("spark.jars", "/postgresql-42.6.0.jar") \
        .getOrCreate()

from pyspark.sql.functions import col, count

# Driver who won a specific race the most times and the number of times they won
def functionality1(spark, postgres_properties):
    # Load the necessary tables from PostgreSQL
    races_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="races", properties=postgres_properties)
    results_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="results", properties=postgres_properties)
    drivers_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="drivers", properties=postgres_properties)
    
    # Get the race name from the user
    race_name = input("Enter the race name: ")
    
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


def functionality3(spark, postgres_properties):
    # Load the necessary tables from PostgreSQL
    races_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="races", properties=postgres_properties)
    results_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="results", properties=postgres_properties)
    drivers_df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/f1db", table="drivers", properties=postgres_properties)
    
    # Get the race name and year from the user
    race_name = input("Enter the race name: ")
    race_year = int(input("Enter the race year: "))
    
    # Join the tables and filter for the specified race
    race_results_df = (races_df.join(results_df, "raceId")
                       .join(drivers_df, "driverId")
                       .filter((col("name") == race_name) & (col("year") == race_year))
                       .select("year", "name", "forename", "surname", "position", "positionText", "points", "laps", "time")
                       .orderBy(col("position").asc_nulls_last()))
    
    # Show the result
    print(f"\nResults for {race_name} {race_year}:")
    race_results_df.show(truncate=False)
    
    return race_results_df


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
        functionality = int(input("Enter the functionality number: "))

        if functionality == 1:
            functionality1(spark, postgres_properties)
        elif functionality == 2:
            functionality2(spark, postgres_properties)
        elif functionality == 3:
            functionality3(spark, postgres_properties)
        else:
            print(f"Invalid functionality number: {functionality}")


    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()