# The date of creation of scripts 06/jan/24

# Version A01 --06/JAN/24

# Import necessary libraries
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import matplotlib.pyplot as plt

'''

QUERY TO EXTRACT DATA FROM FROM FOR MACHINE LEARNING MODEL
   ELECT *
FROM   (SELECT symbol,
               date1,
               close_price,
               ttl_trd_qnty
        FROM   daily_nse_all
        WHERE  symbol = 'IDEA'
        UNION
        SELECT symbol,
               date1,
               close_price,
               ttl_trd_qnty
        FROM   daily_nse_all_hist
        WHERE  symbol = 'IDEA')
ORDER  BY date1 DESC

'''

# Load historical stock price data (replace 'your_data.csv' with your dataset)
data = pd.read_csv('D:\\EagleEye\\idea.csv')

# Display the first few rows of the dataset
print(data.head())
print(data['DATE1'])

data['DATE1'] = pd.to_datetime(data['DATE1'])


# Feature engineering (you may need to customize this based on your dataset)
#data['Date'] = pd.to_datetime(data['Date1'])
data['Day'] = data['DATE1'].dt.day
#data['TTL_TRD_QNTY'] = data['TTL_TRD_QNTY']

#data['Month'] = data['DATE1'].dt.month
#data['Year'] = data['DATE1'].dt.year
#data['Weekday'] = data['DATE1'].dt.weekday

# Define features and target variable
features = ['Day','TTL_TRD_QNTY']
target = 'CLOSE_PRICE'  # Adjust based on your dataset

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(data[features], data[target], test_size=0.2, random_state=42)

# Create a linear regression model
model = LinearRegression()

# Train the model
model.fit(X_train, y_train)

# Make predictions on the test set
predictions = model.predict(X_test)

# Evaluate the model
mse = mean_squared_error(y_test, predictions)
print(f'Mean Squared Error: {mse}')

# Visualize the predictions
plt.figure(figsize=(12, 6))
plt.plot(y_test.index, y_test.values, label='Actual Prices', color='blue')
plt.plot(y_test.index, predictions, label='Predicted Prices', color='red')
plt.title('Stock Price Prediction with Linear Regression')
plt.xlabel('Date')
plt.ylabel('Stock Price')
plt.legend()
plt.show()
