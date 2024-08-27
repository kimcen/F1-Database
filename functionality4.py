from pyspark.sql.functions import sum, col, max, desc
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

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