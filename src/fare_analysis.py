from pyspark.sql.functions import avg, col

def analyze_fares(df):
    # Average fare by pickup and dropoff location
    avg_fare_by_location = df.groupBy("PULocationID", "DOLocationID").avg("fare_amount").orderBy("avg(fare_amount)", ascending=False)
    avg_fare_by_location.show(20, truncate=False)

    # Average fare by passenger count
    avg_fare_by_passenger_count = df.groupBy("passenger_count").avg("fare_amount").orderBy("avg(fare_amount)", ascending=False)
    avg_fare_by_passenger_count.show(20, truncate=False)

    # Correlation between fare amount and trip distance
    corr_fare_distance = df.stat.corr("fare_amount", "trip_distance")
    print(f"Correlation between fare amount and trip distance: {corr_fare_distance}")
