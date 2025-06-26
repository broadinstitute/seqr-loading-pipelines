import numpy as np


def convert_ndarray_to_list(obj):
    if isinstance(obj, np.ndarray):
        return [convert_ndarray_to_list(item) for item in obj.tolist()]
    if isinstance(obj, dict):
        return {k: convert_ndarray_to_list(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_ndarray_to_list(item) for item in obj]
    return obj
