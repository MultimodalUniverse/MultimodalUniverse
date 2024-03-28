import h5py
import shap
import matplotlib.pyplot as plt
import numpy as np

import xgboost as xgb

from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
from sklearn.model_selection import train_test_split

if __name__ == "__main__":
    # Hyperparameters
    SEED = 42
    DATA_PATH = "../../data/yse.hdf5"

    with h5py.File(DATA_PATH) as f:
        classes  = f['spec_class'][:]
    arg_keep = np.where(classes != b'NA')

    with h5py.File(DATA_PATH) as f:
        flux     = f['flux'][arg_keep]
        time     = f['time'][arg_keep]
        photo_z  = f['photo_z'][arg_keep]
        classes  = f['spec_class'][arg_keep]

    n_objects = flux.shape[0]
    data      = np.stack([time, flux], axis=2).reshape(n_objects, -1)
    data      = np.concatenate([data, photo_z[:, None]], axis=1)

    type_classes = np.unique(classes)
    n_classes    = len(type_classes)
    class_dict   = dict(zip(type_classes, np.arange(0, n_classes)))
    labels       = np.array([class_dict[c] for c in classes])

    # Split the data
    X_train, X_test, y_train, y_test = train_test_split(data, labels, test_size=0.2, random_state=SEED)

    model = xgb.XGBClassifier(use_label_encoder=False, eval_metric='mlogloss')
    model.fit(X_train, y_train)

    # Evaluate the model
    accuracy = model.score(X_test, y_test)
    print(f"Accuracy: {accuracy}")

    # Confusion matrix
    reverse_dict = {v: k for k, v in class_dict.items()}

    y_pred   = model.predict(X_test)

    test_classes = np.unique(y_test)
    name_classes = [reverse_dict[c].decode("utf-8") for c in test_classes]
    cm       = confusion_matrix(y_test, y_pred)
    disp     = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=name_classes)
    disp.plot()
    plt.show()

