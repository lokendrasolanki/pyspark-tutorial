from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("filter-df").getOrCreate()

# Define data for employees
employee_data = [
    (1, "Amit Kumar", "IT", 75000, "2021-04-12"),
    (2, "Neha Sharma", "HR", 58000, "2020-01-25"),
    (3, "Rahul Verma", "Finance", 85000, "2019-08-15"),
    (4, "Pooja Gupta", "IT", 67000, "2022-06-19"),
    (5, "Arjun Mehta", "Operations", 49000, "2023-02-01"),
    (6, "Riya Singh", "Marketing", 60000, "2021-11-07"),
    (7, "Mohit Joshi", "Finance", 92000, "2018-12-30"),
    (8, "Kavita Rao", "HR", 55000, "2020-09-10"),
    (9, "Vikram Patel", "IT", 72000, "2022-03-15"),
    (10, "Sonal Jain", "Operations", 48000, None)
]

# Define schema for the employee DataFrame
columns = ["EmpID", "Name", "Department", "Salary", "JoiningDate"]

# Create DataFrame
employee_df = spark.createDataFrame(data=employee_data, schema=columns)

employee_df.display()


filter_df = employee_df.filter("Salary>60000")
filter_df.display()

# And , or condition

condition_df = employee_df.filter((col("Salary")>60000 ) & (col("Department")=='IT'))
condition_df.display()

# StartWith and endswith condition
start_with_df = employee_df.filter(col("name").startswith("Am"))
start_with_df.display()
ends_with_df = employee_df.filter(col("name").endswith("ta"))
ends_with_df.display()


# contains condition
contains_df = employee_df.filter(col("name").contains("ta"))
contains_df.display()

# isnull, isNotNull  and isIn condition

is_null_df = employee_df.filter(col("joiningdate").isNull())
is_null_df.display()
is_not_null_df = employee_df.filter(col("joiningdate").isNotNull())
is_not_null_df.display()
is_in_df = employee_df.filter(col("department").isin("IT","Finance"))
is_in_df.display()


# like condition
like_df = employee_df.filter(col("name").like("%Moh%"))
like_df.display()