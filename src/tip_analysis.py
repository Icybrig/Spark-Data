from pyspark.sql.functions import avg, col, dayofweek, hour, month

def analyze_tips(df):
    # Tip percentage
    df = df.withColumn("tip_percentage", (col("tip_amount") / col("fare_amount")) * 100)
    
    # Do some locations tip more than others?
    avg_tip_by_location = df.groupBy("PULocationID").avg("tip_percentage").orderBy("avg(tip_percentage)", ascending=False)
    avg_tip_by_location.show(10, truncate=False)

    # Correlation between PULocationID and avg(tip_percentage)
    pandas_df_location = avg_tip_by_location.toPandas()
    corr_location_tip = pandas_df_location.corr().loc['PULocationID', 'avg(tip_percentage)']
    print(f"Correlation between PULocationID and avg(tip_percentage): {corr_location_tip}")

    # Is there a correlation between distance and tip amount?
    corr_distance_tip = df.stat.corr("trip_distance", "tip_amount")
    print(f"Correlation between trip distance and tip amount: {corr_distance_tip}")

    # Average tip amount by time of day
    avg_tip_by_hour = df.groupBy(hour("tpep_pickup_datetime").alias("hour")).avg("tip_amount").orderBy("hour")
    avg_tip_by_hour.show(24, truncate=False)

    # Average tip amount by day of week
    avg_tip_by_day = df.groupBy(dayofweek("tpep_pickup_datetime").alias("day_of_week")).avg("tip_amount").orderBy("day_of_week")
    avg_tip_by_day.show(7, truncate=False)

    # Average tip amount by month of year
    avg_tip_by_month = df.groupBy(month("tpep_pickup_datetime").alias("month")).avg("tip_amount").orderBy("month")
    avg_tip_by_month.show(12, truncate=False)

    # Does the payment type affect tipping?
    avg_tip_by_payment_type = df.groupBy("payment_type").avg("tip_amount").orderBy("avg(tip_amount)", ascending=False)
    avg_tip_by_payment_type.show(10, truncate=False)
