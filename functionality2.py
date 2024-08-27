from pyspark.sql.functions import count, col
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

# Returns the number or races per season and saves it as a line plot
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