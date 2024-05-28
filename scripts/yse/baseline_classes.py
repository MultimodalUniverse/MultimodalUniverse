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

# Labels Classes
classes = np.unique( dset['obj_type'] )
dict_class = { classes[i]: i for i in range(len(classes)) }
class_dict = { i: classes[i] for i in range(len(classes)) }

labels = np.array([dict_class[i] for i in dset['obj_type']])

# Reshape the data to (N, M*4)
N, M, F = data.shape
data = data.reshape(N, M * F)

# Split the data into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(data, labels, test_size=0.2, random_state=42)

# Create the XGBoost classifier
model = xgb.XGBClassifier(
    objective='multi:softmax',  # For multiclass classification
    num_class=22,  # Number of classes
    eval_metric='mlogloss',  # Evaluation metric
    use_label_encoder=False
)

# Train the model
model.fit(X_train, y_train)

# Predict the labels for the test set
y_pred = model.predict(X_test)

# Evaluate the model
accuracy = accuracy_score(y_test, y_pred)
print(f'Accuracy: {accuracy * 100:.2f}%')
