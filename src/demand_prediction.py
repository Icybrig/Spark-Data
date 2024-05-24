from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

def demand_prediction(df):
    # Feature engineering
    df = df.withColumn('hour_of_day', F.hour('tpep_pickup_datetime'))
    df = df.withColumn('day_of_week', F.dayofweek('tpep_pickup_datetime'))
    df = df.withColumn('month_of_year', F.month('tpep_pickup_datetime'))
    df = df.withColumn('unix_time', F.unix_timestamp('tpep_pickup_datetime'))

    # Group by time windows to aggregate pickup counts
    windowSpec = Window.orderBy("unix_time").rangeBetween(-3600, 0)
    df = df.withColumn('pickup_count_hourly', F.count('VendorID').over(windowSpec))

    # Create feature vector
    feature_cols = ['hour_of_day', 'day_of_week', 'month_of_year']
    assembler = VectorAssembler(inputCols=feature_cols, outputCol='features')
    df = assembler.transform(df)

    # Prepare training data
    train_df = df.select('features', 'pickup_count_hourly').withColumnRenamed('pickup_count_hourly', 'label')

    # Train the regression model
    lr = LinearRegression(featuresCol='features', labelCol='label')
    lr_model = lr.fit(train_df)

    # Show model summary
    lr_summary = lr_model.summary
    print(f"R2: {lr_summary.r2}, RMSE: {lr_summary.rootMeanSquaredError}")

    # Predict on new data
    predictions = lr_model.transform(train_df)
    predictions.select('features', 'label', 'prediction').show(10)

    return predictions
