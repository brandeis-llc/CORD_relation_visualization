import time
import argparse
from os import path
import pandas as pd

from elastic_index import ESIndex
from data.meta import ParseMetaData
from data.doc import ParseJsonDoc


def load_es_index(index_name, data_dir: str, meta_file: str):
    """
    build es index using COVID meta csv as the main entry
    :param index_name: es index name
    :param data_dir: directory where you have the meta csv
    :param meta_file: meta csv file name
    :return:
    """
    meta_parser = ParseMetaData()
    st = time.time()
    csv_df = pd.read_csv(path.join(data_dir, meta_file))
    docs = []

    for i, item in enumerate(csv_df.iloc):
        item_dict = item.to_dict()  # read in each line from meta csv and convert it into a meta dict
        doc_parser = ParseJsonDoc(data_dir, item_dict['sha'])
        if doc_parser.fields:
            item_dict.update(doc_parser.fields)  # parse each corresponding json doc and update the meta dict
        meta_parser(item_dict)  # further parse the updated meta dict
        docs.append(meta_parser.meta_dict)
    ESIndex(index_name, docs)
    print(f"=== Built {index_name} in {round(time.time() - st, 4)} seconds ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('index_name')
    parser.add_argument('data_dir')
    parser.add_argument('meta_path')
    args = parser.parse_args()
    load_es_index(args.index_name, args.data_dir, args.meta_path)
    # load_es_index('covid_meta_index', 'raw_data', 'sub_meta.csv')
