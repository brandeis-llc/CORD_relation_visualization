import time
import pickle
import argparse
from collections import defaultdict

from elastic_index import ESIndex

from data.parse_pmc_stats import ParsePMCStmts


def load_es_index(index_name, ppi_parser: ParsePMCStmts, docs_input='raw_data/ppi_docs.pkl'):
    """
    build es index using chem_gene_ixns_relation.csv
    """
    docs = []
    docs_dict = defaultdict(list)
    try:
        with open(docs_input, 'rb') as f:
            docs = pickle.load(f)
    except FileNotFoundError:
        for i, (pmid, ppi_doc) in enumerate(ppi_parser.generate_evidence()):
            docs.append({'pubmed_id': pmid + '-' + str(i), 'PPIs': ppi_doc})
            # docs_dict[pmid].append(ppi_doc)
            if (i+1) % 10000 == 0:
                print(f'loading {i+1} documents...')
        # for pmid in docs_dict:
        #     docs.append({'pubmed_id': pmid, 'PPIs': docs_dict[pmid]})
        with open(docs_input, 'wb') as f:
            pickle.dump(docs, f)
        print(f'Writing docs to {docs_input}...')
    st = time.time()
    ESIndex(index_name, docs)
    print(f'building index ...')
    print(f"=== Built {index_name} in {round(time.time() - st, 2)} seconds ===")


if __name__ == "__main__":

    pmc_stmts_path = 'raw_data/2020-03-20-john/cord19_pmc_stmts_filt.pkl'
    ppi_parser = ParsePMCStmts(pmc_stmts_path)
    parser = argparse.ArgumentParser()
    parser.add_argument('index_name')
    args = parser.parse_args()
    load_es_index(args.index_name, ppi_parser)
