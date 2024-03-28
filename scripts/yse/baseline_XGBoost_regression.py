import h5py
import shap
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

import xgboost as xgb

from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split

if __name__ == "__main__":
    # Hyperparameters
    SEED = 42
    DATA_PATH = "../../data/yse.hdf5"

    with h5py.File(DATA_PATH) as f:
        photo_z  = f['photo_z'][:]
    arg_keep = np.where(photo_z >= 0)

    with h5py.File(DATA_PATH) as f:
        flux     = f['flux'][arg_keep]
        time     = f['time'][arg_keep]
        photo_z  = f['photo_z'][arg_keep]

    n_objects = flux.shape[0]
    data      = np.stack([time, flux], axis=2).reshape(n_objects, -1)
    labels    = photo_z

    # Split the data
    X_train, X_test, y_train, y_test = train_test_split(data, labels, test_size=0.2, random_state=SEED)

    model = xgb.XGBRegressor(objective ='reg:squarederror')
    model.fit(X_train, y_train)

    # Predictions
    predictions = model.predict(X_test)

    # Compute and print the Mean Squared Error
    mse = mean_squared_error(y_test, predictions)
    print("MSE: %f" % (mse))

    # Plot results
    # plt.scatter(y_test, predictions)
    sns.jointplot(x=y_test, y=predictions, kind="kde")
    plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'k--')
    plt.xlabel('True')
    plt.ylabel('Predicted')
    plt.show()

