# Spark-Data
# Installment Requirements
!pip install requirements.txt

run the Makefile in the gcp terminal to create the cluster and the bucket

![cc 1](https://github.com/Icybrig/Spark-Data/assets/136721036/6e9c1501-146c-45b0-8186-4eeeee4d5b23)

# Report of Final Assignment Preparation

Preparation for the environment settings:
We have two functions which one of them used to set the environment settings and the other one to delete the environment settings.

Preparation for the Spark:
There is the code in the main.ipynb to launch the spark session and load the data into it. 

Preparation for the dataset:
We have combined those datasets into one which named 'combined_df' and to do the analysis based on it

# Report of Final Assignment Analysis

1.Trip Analysis

Average duration and distance of rides: Compare these metrics by time of day, day of week,
and month of year. This can reveal patterns such as longer trips during rush hours, on
weekends, or during holiday seasons.

Answer: Yes, based on our analysis. For the hours, we can see that when it is in the afternoon between 12 - 24, the average trip distance is quiet low probably due to the traffic situation. But in the morning it can be achieved in 45 or 43 in 4 o'clock or 5 o'clock. For day of week, there is no significant diffrence between weekend and working days. For the month of year, except first three months, the rest of distance travelled are similar. But as close as to the end of the year, the time spent on the trip will be longer probably because there are more vacations in the last-half year. 

Popular locations: Identify the top 10 pickup and dropoff locations. This could be interesting
when mapped visually.

Answer: Showed in the main.ipynb

2.Tip Analysis

Tip percentage by trip: Do some locations tip more than others? Is there a correlation between
distance and tip?

Answer: Yes. There are two locations are much higher than the others. PULocationID 252 and 1. For the correlation between distance and tip, it looks like people are not willing to go to somewhere to far away. For trip distance between 1.45 and 3, there are more people than other locations.

Tips by time: Does the time of day, week, or even year affect tipping behavior? You could
cross-reference this with holidays or events.

Answer: For the hour and the day of the week, there is no such significant difference between those. But for the month, we can see that close the end of the year will be more tip amount. It probably because of the holiday such as Christmas. 

Does the payment type affect the tipping?

Answer: Yes. For the payment type 0, 1 and 2, the tip amount is much higher than the rest. For the payment type 3, it is even negative. 

3.Fare Analysis

Can you calculate the average fare by pull & drop location?

Answer: Yes, showed in the main.ipynb.

Can you calculate the average fare by Passenger count ? to see if there is any correlation with
passenger count and fare amount

Answer: Yes. When the passenger count is 7, 8 or 9, the fare amount are much higher than the others.

Can you correlate the fare amount and the distance trip ?

Answer: The correlation between the fare amount and the distance trip is weak. To see in the main.ipynb.

4.Traffic Analysis

Trip speed: Create a new feature for the average speed of a trip, and use this to infer traffic
conditions by trying to find if for similar trip (when they exist) they more or less have the same
avg speed or not, try then to group the avg speed by trip then hour, day, week

Answer: Yes. In the main.ipynb.

5.Demand Prediction

Feature engineering: Use the date and time of the pickups to create features for the model, such as
hour of the day, day of the week, etc.

Regression model: Use a regression model (such as linear regression) to predict the number of
pickups in the next hour based on the features.

Answer: We have done the prediction and ran in the main.ipynb. But due to the size of the dataset is too large to print. Therefore, we only show the prediction results of top 10 rows with Features, Labels and Prediction. Check in main.ipynb please.

