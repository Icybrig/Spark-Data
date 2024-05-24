from pyspark.sql.functions import col, count, when, isnull

def handle_missing_values(df):
    total_rows = df.count()
    median_passenger_count = df.stat.approxQuantile("passenger_count", [0.5], 0.25)[0]
    mode_ratecodeid = df.groupBy("RatecodeID").count().orderBy("count", ascending=False).first()["RatecodeID"]
    
    df = df.fillna({
        "passenger_count": median_passenger_count,
        "RatecodeID": mode_ratecodeid,
        "store_and_fwd_flag": 'N',
        "congestion_surcharge": 0,
        "airport_fee": 0
    })
    return df

def calculate_nan_percentage(df):
    total_rows = df.count()
    nan_percentage = df.select([(count(when(isnull(c), c)) / total_rows).alias(c) for c in df.columns])
    nan_percentage.show(vertical=True)
