from pyspark.sql.functions import hour, dayofweek, month, unix_timestamp, col

def add_trip_duration(df):
    return df.withColumn('trip_duration', 
                         (unix_timestamp(col('tpep_dropoff_datetime')) - unix_timestamp(col('tpep_pickup_datetime'))) / 60)

def analyze_trip(df):
    # Add trip duration column
    df = add_trip_duration(df)
    
    # Average duration and distance of rides by time of day
    df.groupBy(hour('tpep_pickup_datetime').alias('hour')).avg('trip_distance', 'trip_duration').orderBy('hour').show(24, truncate=False)

    # Average duration and distance of rides by day of week
    df.groupBy(dayofweek('tpep_pickup_datetime').alias('day_of_week')).avg('trip_distance', 'trip_duration').orderBy('day_of_week').show(7, truncate=False)

    # Average duration and distance of rides by month of year
    df.groupBy(month('tpep_pickup_datetime').alias('month')).avg('trip_distance', 'trip_duration').orderBy('month').show(12, truncate=False)

    # Top 10 pickup locations
    df.groupBy('PULocationID').count().orderBy('count', ascending=False).show(10, truncate=False)

    # Top 10 dropoff locations
    df.groupBy('DOLocationID').count().orderBy('count', ascending=False).show(10, truncate=False)
