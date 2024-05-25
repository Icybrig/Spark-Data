# Spark-Data
# Installment Requirements
!pip install requirements.txt

run the Makefile in the gcp terminal to create the cluster and the bucket

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

![352b7da7accc3993c8c7333fe9c4948](https://github.com/Icybrig/Spark-Data/assets/136721036/8533505b-582f-47c2-84ce-0b1102df48dd)
![d55462867ac8138cb9ab3f5c7ac6917](https://github.com/Icybrig/Spark-Data/assets/136721036/d4383f0a-f67b-4f63-bac5-ea0205e916e4)

Answer: Yes, based on our analysis. For the hours, we can see that when it is in the afternoon between 12 - 24, the average trip distance is quiet low probably due to the traffic situation. But in the morning it can be achieved in 45 or 43 in 4 o'clock or 5 o'clock. For day of week, there is no significant diffrence between weekend and working days. For the month of year, except first three months, the rest of distance travelled are similar. But as close as to the end of the year, the time spent on the trip will be longer probably because there are more vacations in the last-half year. 

Popular locations: Identify the top 10 pickup and dropoff locations. This could be interesting
when mapped visually.

![289caecdf56739942ea29be64b3f739](https://github.com/Icybrig/Spark-Data/assets/136721036/8d208b12-490d-45a7-922a-e95f09096e50)

Answer: Showed in the main.ipynb

2.Tip Analysis

Tip percentage by trip: Do some locations tip more than others? Is there a correlation between
distance and tip?

![e6c45d0603a92f745eb75aaf7fe79ec](https://github.com/Icybrig/Spark-Data/assets/136721036/75589fbd-50e9-470f-b659-53a49df35a8c)

Answer: Yes. There are two locations are much higher than the others. PULocationID 252 and 1. For the correlation between distance and tip, it looks like people are not willing to go to somewhere to far away. For trip distance between 1.45 and 3, there are more people than other locations.

Tips by time: Does the time of day, week, or even year affect tipping behavior? You could
cross-reference this with holidays or events.

![930ac0b7d62ebcd6d73e341e13da29e](https://github.com/Icybrig/Spark-Data/assets/136721036/cd363ba4-68b2-4220-a2d8-dce3279a1c7c)
![6fd8c8e11fe6066f10656a37e5dc844](https://github.com/Icybrig/Spark-Data/assets/136721036/8aab6e1c-9a10-4a42-a8a4-917fb413ab1a)

Answer: For the hour and the day of the week, there is no such significant difference between those. But for the month, we can see that close the end of the year will be more tip amount. It probably because of the holiday such as Christmas. 

Does the payment type affect the tipping?

![cb500dcd4913eed1b2a3e078812cfd7](https://github.com/Icybrig/Spark-Data/assets/136721036/3916838e-46cb-41b3-b504-8e68d41003b6)

Answer: Yes. For the payment type 0, 1 and 2, the tip amount is much higher than the rest. For the payment type 3, it is even negative. 

3.Fare Analysis

Can you calculate the average fare by pull & drop location?

![7876b17c2a0edd830efea4729b8fd1c](https://github.com/Icybrig/Spark-Data/assets/136721036/148c350e-11cc-42c8-851c-6fb5d107fe93)

Answer: Yes, showed in the main.ipynb.

Can you calculate the average fare by Passenger count ? to see if there is any correlation with
passenger count and fare amount

![f6724c596122af53f49a501e65acbba](https://github.com/Icybrig/Spark-Data/assets/136721036/e56a35a1-8a6a-4539-9eb7-d7f1544016f7)

Answer: Yes. When the passenger count is 7, 8 or 9, the fare amount are much higher than the others.

Can you correlate the fare amount and the distance trip ?

![0caa963c50a7b210a3cb9dfd96b56f7](https://github.com/Icybrig/Spark-Data/assets/136721036/38ca1a39-3573-46c3-b0fe-96b38e2fc9d0)

Answer: The correlation between the fare amount and the distance trip is weak. To see in the main.ipynb.

4.Traffic Analysis

Trip speed: Create a new feature for the average speed of a trip, and use this to infer traffic
conditions by trying to find if for similar trip (when they exist) they more or less have the same
avg speed or not, try then to group the avg speed by trip then hour, day, week

![b0cd2ffbe07c86a07ea630dfa94f310](https://github.com/Icybrig/Spark-Data/assets/136721036/a98528af-92d4-4ccb-b529-9e6027923176)
![bceded1b84850e23fd5b2285d9ce565](https://github.com/Icybrig/Spark-Data/assets/136721036/52175ba8-d2bc-4925-8cc7-2a5cc85ac83f)
![29b24da9f81752e0bfc71360f249c57](https://github.com/Icybrig/Spark-Data/assets/136721036/e4f13ec6-786f-4e2a-a239-d47ba3f1f194)
![e4718a079977f527d8c521060ce4b70](https://github.com/Icybrig/Spark-Data/assets/136721036/457e4b13-18aa-49bd-b854-548834feee1a)

Answer: Yes. In the main.ipynb.

5.Demand Prediction

Feature engineering: Use the date and time of the pickups to create features for the model, such as
hour of the day, day of the week, etc.

Regression model: Use a regression model (such as linear regression) to predict the number of
pickups in the next hour based on the features.

![492fccec8e7a38f630b661c6e82e70a](https://github.com/Icybrig/Spark-Data/assets/136721036/2c6b6a74-e072-4a79-86ce-a90efff0a704)

Answer: We have done the prediction and ran in the main.ipynb. But due to the size of the dataset is too large to print. Therefore, we only show the prediction results of top 10 rows with Features, Labels and Prediction. Check in main.ipynb please.

# Results of Cluster

![cc 1](https://github.com/Icybrig/Spark-Data/assets/136721036/6e9c1501-146c-45b0-8186-4eeeee4d5b23)