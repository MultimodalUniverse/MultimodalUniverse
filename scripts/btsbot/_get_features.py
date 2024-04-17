
import h5py

def _get_features(example_file_path):
    _FLOAT_FEATURES = []
    _INT_FEATURES = []
    _BOOL_FEATURES = []
    _STRING_FEATURES = []

    with h5py.File(example_file_path, 'r') as f:
        fields = f['table'].dtype.fields
        for field in fields:
            if fields[field][0].kind == 'f':
                _FLOAT_FEATURES.append(field)
            elif fields[field][0].kind == 'i':
                _INT_FEATURES.append(field)
            elif fields[field][0].kind == 'b':
                _BOOL_FEATURES.append(field)
            elif fields[field][0].kind == 'S':
                _STRING_FEATURES.append(field)

    return _FLOAT_FEATURES, _INT_FEATURES, _BOOL_FEATURES, _STRING_FEATURES
