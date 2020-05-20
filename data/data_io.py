from typing import Any
import pickle


def pickle_obj_mapping(obj: Any, path: str) -> None:
    with open(path, "wb") as file:
        return pickle.dump(obj, file, protocol=pickle.HIGHEST_PROTOCOL)


def load_pickled_obj(path: str) -> Any:
    with open(path, "rb") as file:
        return pickle.load(file)
