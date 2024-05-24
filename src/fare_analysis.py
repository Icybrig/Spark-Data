def analyze_fares(df):
    df.groupBy('PULocationID', 'DOLocationID').avg('fare_amount').show()
    df.groupBy('passenger_count').avg('fare_amount').show()
    print(df.corr('fare_amount', 'trip_distance'))
