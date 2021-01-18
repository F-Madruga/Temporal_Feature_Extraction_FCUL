# Machine-Learning Temporal Feature Extraction

In the context of detecting financial fraud, a common approach is to build new temporal features in order to apply machine-learning algorithms. In this project you will take a dataset consisting of several transactions, each with a timestamp and several columns representing different raw features (such as [time, year, month, day, amount, buyer, seller, credit_card, latitude, longitude]).

Each row has a single id that represents the timestamp. Features can only use the previous rows in the dataset. If there is no data, you should use the value 0. The row itself is not used to generate the its aggregate metrics.

The goal is to extract the following aggregate features as fast as possible. The result of your program should be a dataset equal to the original one, but with the following extra columns:


1) The average amount of all past purchases of that seller.
2) The average amount of all past purchases of that buyer.
3) The average amount of all past purchases of that seller in the current month.
4) The average amount of the past 100 purchases of that seller.
5) The average amount of all past purchases of that seller, plus 3 times the standard deviation.
6) The Euclidean distance between the current position (latitude and longitude) and the last one by the same buyer.
7) Total number of purchases from that credit_card in the past.
8) The average amount of all purchases of that buyer in the past in the same month (even if different years).


Your solution should be scalable to different number of features, different features of the same type, and any number of rows.