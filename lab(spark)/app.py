from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("HDFS Parquet SQL Linear Demo")
    .master("spark://spark-master:7077")
    .config("spark.hadoop.fs.defaultFS", "hdfs://hdfs-nn:9000")
    .getOrCreate()
)
print("Spark session started.")

data = [
    {"emp_id": 101, "emp_name": "Emma", "age": 29, "department": "Finance"},
    {"emp_id": 102, "emp_name": "Liam", "age": 34, "department": "IT"},
    {"emp_id": 103, "emp_name": "Olivia", "age": 31, "department": "HR"},
    {"emp_id": 104, "emp_name": "Noah", "age": 27, "department": "Finance"},
    {"emp_id": 105, "emp_name": "Ava", "age": 36, "department": "IT"}
]

df = spark.createDataFrame(data)
print("Sample data created:")
df.show()

parquet_path = "hdfs://hdfs-nn:9000/user/data/employees_parquet"
print(f"Writing Parquet to HDFS: {parquet_path}")
df.write.mode("overwrite").parquet(parquet_path)
print("Parquet file saved to HDFS.")

print(f"Reading Parquet from HDFS: {parquet_path}")
parquet_df = spark.read.parquet(parquet_path)

parquet_df.createOrReplaceTempView("employees")
print("Temporary SQL view 'employees' registered.")

print("\nAll employees:")
spark.sql("SELECT * FROM employees").show()

print("\nEmployees in IT department:")
spark.sql("SELECT emp_name, age FROM employees WHERE department = 'IT'").show()

print("\nAverage age by department:")
spark.sql("SELECT department, AVG(age) AS avg_age FROM employees GROUP BY department").show()

spark.stop()
print("Spark session stopped.")
