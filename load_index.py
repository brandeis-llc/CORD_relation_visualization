import time
import argparse
from os import path
import pandas as pd

from elastic_index import ESIndex
from data.meta import ParseMetaData
from data.doc import ParseJsonDoc
from data.chem_gene_rel import ParseChemGeneRel


def load_es_index(index_name, data_dir: str, meta_file: str, rel_parser):
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
    csv_df = csv_df.astype({'pubmed_id': 'int32'})
    docs = []
    print(f'Building ES index for {len(csv_df)} documents...')

    for i, item in enumerate(csv_df.iloc):
        item = item.fillna('')
        item_dict = item.to_dict()  # read in each line from meta csv and convert it into a meta dict
        doc_parser = ParseJsonDoc(data_dir, item_dict['sha'])
        if doc_parser.fields:
            item_dict.update(doc_parser.fields)  # parse each corresponding json doc and update the meta dict
        item_dict.update(rel_parser(str(item_dict['pubmed_id'])))  # add interaction actions extracted from each article
        meta_parser(item_dict)  # further parse the updated meta dict
        docs.append(meta_parser.meta_dict)
        if (i + 1) % 1000 == 0:
            print(f'finish loading {i + 1} documents ...')
    ESIndex(index_name, docs)
    print(f"=== Built {index_name} in {round(time.time() - st, 4)} seconds ===")


if __name__ == "__main__":
    gene_mapping_path = 'raw_data/genes_mapping.pkl'
    chem_mapping_path = 'raw_data/chem_mapping.pkl'
    chem_gen_rel_path = 'raw_data/KG/chem_gene_ixns_relation.csv'
    rel_parser = ParseChemGeneRel(chem_gen_rel_path, gene_mapping_path, chem_mapping_path)
    parser = argparse.ArgumentParser()
    parser.add_argument('index_name')
    parser.add_argument('data_dir')
    parser.add_argument('meta_path')
    args = parser.parse_args()
    load_es_index(args.index_name, args.data_dir, args.meta_path, rel_parser)
    # load_es_index('covid_meta_index', 'raw_data', 'sub_meta.csv')
