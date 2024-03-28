import h5py
import xgboost as xgb

from sklearn.model_selection import train_test_split


if __name__ == "__main__":
    # Hyperparameters
    SEED = 42
    DATA_PATH = "yse_dr1.hdf5"
    
    # TODO: Load the data
    with h5py.File(DATA_PATH, "r") as f:
        X = f["X"][:]
        y = f["y"][:]

    # Split the data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=SEED)

    # Train the model
    model = xgb.XGBClassifier()
    model.fit(X_train, y_train)

    # Evaluate the model
    accuracy = model.score(X_test, y_test)
    print(f"Accuracy: {accuracy}")