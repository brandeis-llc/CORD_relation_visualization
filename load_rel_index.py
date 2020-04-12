import time
import pickle
import argparse
from collections import defaultdict

from elastic_index import ESIndex

from data.chem_gene_rel import ParseChemGeneRel


def load_es_index(index_name, rel_parser, docs_input='raw_data/rel_docs.pkl'):
    """
    build es index using chem_gene_ixns_relation.csv
    """
    docs = []
    docs_dict = defaultdict(list)
    try:
        with open(docs_input, 'rb') as f:
            docs = pickle.load(f)
    except FileNotFoundError:
        for i, (rel_doc, pmids_lst) in enumerate(rel_parser):
            for pmid in pmids_lst:
                docs_dict[pmid].append(rel_doc)
            if (i+1) % 50000 == 0:
                print(f'loading {i+1} documents...')
        for pmid in docs_dict:
            docs.append({'pubmed_id': pmid, 'action_interactions': docs_dict[pmid]})
        with open(docs_input, 'wb') as f:
            pickle.dump(docs, f)
        print(f'Writing docs to {docs_input}...')
    st = time.time()
    ESIndex(index_name, docs)
    print(f'building index ...')
    print(f"=== Built {index_name} in {round(time.time() - st, 2)} seconds ===")


if __name__ == "__main__":
    gene_mapping_path = 'raw_data/genes_mapping.pkl'
    chem_mapping_path = 'raw_data/chem_mapping.pkl'
    chem_gen_rel_path = 'raw_data/KG/chem_gene_ixns_relation.csv'
    rel_parser = ParseChemGeneRel(chem_gen_rel_path, gene_mapping_path, chem_mapping_path)
    parser = argparse.ArgumentParser()
    parser.add_argument('index_name')
    args = parser.parse_args()
    load_es_index(args.index_name, rel_parser)
