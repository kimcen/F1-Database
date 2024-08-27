from functionality1 import functionality1
from functionality2 import functionality2
from functionality3 import functionality3
from functionality4 import functionality4
from functionality5 import functionality5
from functionality6 import functionality6
from pyspark.sql import SparkSession
import psycopg2
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