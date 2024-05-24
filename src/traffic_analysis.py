from pyspark.sql.functions import col, hour, dayofweek, month, avg

def analyze_traffic(df):
    df = df.withColumn('trip_duration', (col('tpep_dropoff_datetime').cast('long') - col('tpep_pickup_datetime').cast('long')) / 60)
    df = df.withColumn('trip_speed', col('trip_distance') / (col('trip_duration') / 60))
    
    threshold = 1
    
    # Aggregating by hour
    df_speed_hour = df.groupBy(hour("tpep_pickup_datetime").alias("hour")) \
        .agg(
            avg("trip_duration").alias("avg_duration"),
            avg("trip_distance").alias("avg_distance"),
            avg("trip_speed").alias("avg_speed")
        ) \
        .orderBy("hour")
    
    # Aggregating by day of the week
    df_speed_week = df.groupBy(dayofweek("tpep_pickup_datetime").alias("week")) \
        .agg(
            avg("trip_duration").alias("avg_duration"),
            avg("trip_distance").alias("avg_distance"),
            avg("trip_speed").alias("avg_speed")
        ) \
        .orderBy("week")
    
    # Applying threshold filters for weekly aggregation
    lower_duration = col("avg_duration") - threshold
    upper_duration = col("avg_duration") + threshold
    lower_distance = col("avg_distance") - threshold
    upper_distance = col("avg_distance") + threshold
    
    df_speed_week_filtered = df_speed_week.filter(
        (lower_duration <= col("avg_duration")) &
        (col("avg_duration") <= upper_duration) &
        (lower_distance <= col("avg_distance")) &
        (col("avg_distance") <= upper_distance)
    )
    
    df_speed_week_pandas = df_speed_week_filtered.toPandas()
    
    # Aggregating by month
    df_speed_month = df.groupBy(month("tpep_pickup_datetime").alias("month")) \
        .agg(
            avg("trip_duration").alias("avg_duration"),
            avg("trip_distance").alias("avg_distance"),
            avg("trip_speed").alias("avg_speed")
        ) \
        .orderBy("month")
    
    # Applying threshold filters for monthly aggregation
    lower_duration = col("avg_duration") - threshold
    upper_duration = col("avg_duration") + threshold
    lower_distance = col("avg_distance") - threshold
    upper_distance = col("avg_distance") + threshold
    
    df_speed_month_filtered = df_speed_month.filter(
        (lower_duration <= col("avg_duration")) &
        (col("avg_duration") <= upper_duration) &
        (lower_distance <= col("avg_distance")) &
        (col("avg_distance") <= upper_distance)
    )
    
    df_speed_month_pandas = df_speed_month_filtered.toPandas()
    
    return df_speed_hour.toPandas(), df_speed_week_pandas, df_speed_month_pandas