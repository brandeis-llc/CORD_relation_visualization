import time
import pickle
import argparse
from typing import List, Dict
from collections import defaultdict

from elastic_index import ESIndex
import spacy

from data.parse_pmc_stats import ParsePMCStmts
from data.meta import ParseMetaData


def load_es_index(
    index_name,
    ppi_parser: ParsePMCStmts,
    meta_input: str,
    docs_input="raw_data/ppi_docs.pkl",
):
    """
    build es index using cord19_pmc_stmts_filt.pkl
    """
    docs = []
    try:
        with open(docs_input, "rb") as f:
            docs = pickle.load(f)
    except FileNotFoundError:
        meta_parser = ParseMetaData()
        with open(meta_input, "rb") as f:
            meta_data: List[Dict] = pickle.load(f)
            print(f"loading {meta_input}...")
        pmid_oriented_meta = {item["pubmed_id"]: item for item in meta_data}
        for i, (pmid, ppi_doc) in enumerate(ppi_parser.generate_evidence_dict()):
            tmp_doc = {"pubmed_id": pmid, "PPIs": ppi_doc, "doc_id": pmid + "-" + str(i)}
            meta_doc = pmid_oriented_meta.get(pmid, {}).copy()
            meta_parser(meta_doc)
            tmp_doc.update(meta_parser.meta_dict)
            docs.append(tmp_doc)
            if (i + 1) % 10000 == 0:
                print(f"loading {i+1} documents...")
        with open(docs_input, "wb") as f:
            pickle.dump(docs, f)
        print(f"Writing docs to {docs_input}...")
    st = time.time()
    ESIndex(index_name, docs)
    print(f"building index ...")
    print(f"=== Built {index_name} in {round(time.time() - st, 2)} seconds ===")


if __name__ == "__main__":

    pmc_stmts_path = "raw_data/2020-03-20-john/statements_2020-05-18-17-11-32.json"
    meta_input_path = "raw_data/sub_metadata.pkl"
    nlp = spacy.load("en_ner_bionlp13cg_md")
    ppi_parser = ParsePMCStmts.from_json(pmc_stmts_path, spacy_model=nlp)
    parser = argparse.ArgumentParser()
    parser.add_argument("index_name")
    parser.add_argument("out_pkl_path")
    args = parser.parse_args()
    load_es_index(args.index_name, ppi_parser, meta_input_path, args.out_pkl_path)
