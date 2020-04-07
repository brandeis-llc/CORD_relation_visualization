import time
import argparse
from collections import defaultdict

from elastic_index import ESIndex

from data.chem_gene_rel import ParseChemGeneRel


def load_es_index(index_name, rel_parser):
    """
    build es index using chem_gene_ixns_relation.csv
    :param index_name: es index name
    :return:
    """
    st = time.time()
    docs = []
    docs_dict = defaultdict(list)

    for i, (rel_doc, pmids_lst) in enumerate(rel_parser):
        for pmid in pmids_lst:
            docs_dict[pmid].append(rel_doc)
        if (i+1) % 10000 == 0:
            print(f'loading {i+1} documents...')

    for pmid in docs_dict:
        docs.append({'pubmed_id': pmid, 'action_interactions': docs_dict[pmid]})
    ESIndex(index_name, docs)
    print(f"=== Built {index_name} in {round(time.time() - st, 4)} seconds ===")


if __name__ == "__main__":
    gene_mapping_path = 'raw_data/genes_mapping.pkl'
    chem_mapping_path = 'raw_data/chem_mapping.pkl'
    chem_gen_rel_path = 'raw_data/KG/chem_gene_ixns_relation.csv'
    rel_parser = ParseChemGeneRel(chem_gen_rel_path, gene_mapping_path, chem_mapping_path)
    parser = argparse.ArgumentParser()
    parser.add_argument('index_name')
    args = parser.parse_args()
    load_es_index(args.index_name, rel_parser)
