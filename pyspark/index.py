from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read CSV with PySpark") \
    .getOrCreate()

# Path to the CSV file
csv_file_path = "data/customers-10000.csv"

# Read the CSV file
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Show the first few rows of the DataFrame
df.show()

# Print the schema of the DataFrame
df.printSchema()

# Stop the SparkSession
spark.stop()