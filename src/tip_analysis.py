from pyspark.sql.functions import avg, col, dayofweek, hour, month

def analyze_tips(df):
    # Tip percentage
    df = df.withColumn("tip_percentage", (col("tip_amount") / col("fare_amount")) * 100)

    # Average tip percentage by pickup location
    df.groupBy("PULocationID").avg("tip_percentage").orderBy("avg(tip_percentage)", ascending=False).show(10, truncate=False)

    # Correlation between distance and tip amount
    df.select("trip_distance", "tip_amount").show(10)
    
    # Average tip amount by time of day
    df.groupBy(hour("tpep_pickup_datetime").alias("hour")).avg("tip_amount").orderBy("hour").show(24, truncate=False)

    # Average tip amount by day of week
    df.groupBy(dayofweek("tpep_pickup_datetime").alias("day_of_week")).avg("tip_amount").orderBy("day_of_week").show(7, truncate=False)

    # Average tip amount by month of year
    df.groupBy(month("tpep_pickup_datetime").alias("month")).avg("tip_amount").orderBy("month").show(12, truncate=False)

    # Average tip amount by payment type
    df.groupBy("payment_type").avg("tip_amount").orderBy("avg(tip_amount)", ascending=False).show(10, truncate=False)
