from pyspark.sql.functions import col, hour, dayofweek, weekofyear, avg, unix_timestamp

def analyze_traffic(df):
    # Ensure the datetime columns are in timestamp format
    df = df.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp"))
    df = df.withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))
    
    # Calculate trip duration in hours
    df = df.withColumn("trip_duration_hours", 
                       (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 3600)
    
    # Calculate average speed in miles per hour
    df = df.withColumn("avg_speed_mph", col("trip_distance") / col("trip_duration_hours"))
    
    # Filter out trips with zero or negative durations
    df = df.filter(col("trip_duration_hours") > 0)
    
    # Show the DataFrame to verify the new columns
    df.select("trip_duration_hours", "avg_speed_mph").show(5)
    
    # Group by trip and calculate average speed
    avg_speed_by_trip = df.groupBy("PULocationID", "DOLocationID").agg(avg("avg_speed_mph").alias("avg_speed")).orderBy("avg_speed", ascending=False)
    avg_speed_by_trip.show(10, truncate=False)

    # Group by hour and calculate average speed
    avg_speed_by_hour = df.groupBy(hour("tpep_pickup_datetime").alias("hour")).agg(avg("avg_speed_mph").alias("avg_speed")).orderBy("hour")
    avg_speed_by_hour.show(24, truncate=False)

    # Group by day of the week and calculate average speed
    avg_speed_by_day = df.groupBy(dayofweek("tpep_pickup_datetime").alias("day_of_week")).agg(avg("avg_speed_mph").alias("avg_speed")).orderBy("day_of_week")
    avg_speed_by_day.show(7, truncate=False)

    # Group by week of the year and calculate average speed
    avg_speed_by_week = df.groupBy(weekofyear("tpep_pickup_datetime").alias("week_of_year")).agg(avg("avg_speed_mph").alias("avg_speed")).orderBy("week_of_year")
    avg_speed_by_week.show(52, truncate=False)
