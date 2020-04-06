from typing import Dict
import pickle


def pickle_obj_mapping(dict_obj: Dict, path: str) -> None:
    with open(path, "wb") as file:
        return pickle.dump(dict_obj, file, protocol=pickle.HIGHEST_PROTOCOL)


def load_pickled_obj(path: str) -> Dict:
    with open(path, "rb") as file:
        return pickle.load(file)
