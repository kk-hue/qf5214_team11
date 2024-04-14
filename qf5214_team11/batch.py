#!/usr/bin/env python
# coding: utf-8

# In[30]:


from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Create a SparkSession
spark = SparkSession.builder.appName("CalculateFinancialIndicators").getOrCreate()

# Read the CSV file and create a DataFrame
df = spark.read.csv("2024-04-04+7_US_ALL.csv", header=True, inferSchema=True)

# Convert the close, volume, high, and low columns to numeric types
df = df.withColumn("Close", F.col("Close").cast("double"))        .withColumn("Volume", F.col("Volume").cast("double"))        .withColumn("High", F.col("High").cast("double"))        .withColumn("Low", F.col("Low").cast("double"))

# Define a window specification, partitioned by Ticker and ordered by datetime
n = 20  # Period for moving average
windowSpec = Window.partitionBy('Ticker').orderBy('Timestamp').rowsBetween(-n+1, 0)

# Calculate the moving average (MA) of close for each Ticker
df = df.withColumn('close_MA', F.avg("Close").over(windowSpec))

# Calculate the moving average (MA) of volume for each Ticker
df = df.withColumn('volume_MA', F.avg("Volume").over(windowSpec))

# Calculate the standard deviation for each Ticker
df = df.withColumn('StdDev', F.stddev("Close").over(windowSpec))

# Calculate the upper and lower Bollinger Bands for each Ticker
k = 2  # Multiplier for Bollinger Bands
df = df.withColumn('UpperBand', df['close_MA'] + (df['StdDev'] * k))
df = df.withColumn('LowerBand', df['close_MA'] - (df['StdDev'] * k))

# Calculate the Stochastic Oscillator, starting with %K
stochastic_window = Window.partitionBy('Ticker').orderBy('Timestamp').rowsBetween(-n+1, 0)
df = df.withColumn('lowestLow', F.min('Low').over(stochastic_window))
df = df.withColumn('highestHigh', F.max('High').over(stochastic_window))
df = df.withColumn('percentK', (F.col('Close') - F.col('lowestLow')) / 
                                  (F.col('highestHigh') - F.col('lowestLow')) * 100)

# Calculate %D, which is the 3-period simple moving average of %K
df = df.withColumn('percentD', F.avg('percentK').over(Window.partitionBy('Ticker').orderBy('Timestamp').rowsBetween(-2, 0)))

output_path = "C:/Users/HP/Desktop/5214/final_results.csv"  # 请替换为您的实际路径

df.coalesce(1).write.option("header", "true").csv(output_path, mode="overwrite")

