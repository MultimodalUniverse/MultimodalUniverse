import numpy as np
from tqdm import tqdm
from datasets import load_dataset 
import matplotlib.pyplot as plt

import pandas as pd
import seaborn as sns
import xgboost as xgb
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

# Load the data
dset = load_dataset('./yse.py', trust_remote_code=True, split='train')
lightcurves = dset['lightcurve']

# Amount of objects
N = len( dset['redshift'] )

# Find max length of Light Curves in the dataset -- 876
max_size = 0
for i in range(N):
    max_size = max(max_size, len(lightcurves[i]['time']))


# Change band names to integers
bands = np.unique( lightcurves[i]['band'] )
dict_band = { bands[i]: i for i in range(len(bands)) }

# Data -- N x max_size x 4
data = np.zeros((N, max_size, 4)) - 1
for i in tqdm(range(N)):
    data[i, :len(lightcurves[i]['band']), 0] = [dict_band[j] for j in lightcurves[i]['band']]
    data[i, :len(lightcurves[i]['band']), 1] = lightcurves[i]['time']
    data[i, :len(lightcurves[i]['band']), 2] = lightcurves[i]['flux']
    data[i, :len(lightcurves[i]['band']), 3] = lightcurves[i]['flux_err']
data[np.where(data[:,:, 1] == -99)] = -1

# Labels Phot-z
labels   = np.array(dset['redshift'])
arg_keep = np.where( (labels >= 0) & (labels <= 0.3) )[0]

data   = data[arg_keep]
labels = labels[arg_keep]


# Reshape the data to (N, M*4)
N, M, F = data.shape
data = data.reshape(N, M * F)

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(data, labels, test_size=0.2, random_state=42)

# Create the XGBoost regressor
model = xgb.XGBRegressor(
    objective='reg:squarederror',  # For regression tasks
    eval_metric='rmse'  # Evaluation metric
)

# Train the model
model.fit(X_train, y_train)

# Predict the labels for the training and test sets
y_train_pred = model.predict(X_train)
y_test_pred = model.predict(X_test)


# Combine the predictions and true values into a single DataFrame
train_df = pd.DataFrame({
    'True Values': y_train,
    'Predicted Values': y_train_pred,
    'Kind': 'Train'
})

test_df = pd.DataFrame({
    'True Values': y_test,
    'Predicted Values': y_test_pred,
    'Kind': 'Test'
})

# Combine the train and test DataFrames
combined_df = pd.concat([train_df, test_df])

# Plot the Prediction vs True for Train (in blue) and Test (in orange)
plt.figure(figsize=(12, 6))

sns.kdeplot(data=combined_df, x="True Values", y="Predicted Values", hue="Kind")

plt.xlabel('True Values')
plt.ylabel('Predicted Values')
plt.title('Prediction vs True Values for Train and Test Sets')
plt.show()