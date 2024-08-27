from pyspark.sql.functions import col, sum
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

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
