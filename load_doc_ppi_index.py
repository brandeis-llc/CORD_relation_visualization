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
    docs_input="raw_data/pmid_ppi.pkl",
):
    """
    build es index using cord19_pmc_stmts_filt.pkl
    """
    docs = []
    try:
        with open(docs_input, "rb") as f:
            ppi_docs = pickle.load(f)
    except FileNotFoundError:
        ppi_docs = ppi_parser.generate_pmid_dict(to_pkl=True)

    meta_parser = ParseMetaData()
    with open(meta_input, "rb") as f:
        meta_data: List[Dict] = pickle.load(f)
        print(f"loading {meta_input}...")
    pmid_oriented_meta = {item["pubmed_id"]: item for item in meta_data}
    for i, pmid in enumerate(ppi_docs):
        tmp_doc = {
            "pubmed_id": pmid,
            "PPIs": ppi_docs[pmid],
            "doc_id": pmid,
            "pmid_url": ppi_docs[pmid][0]["pmid_url"],
        }
        meta_doc = pmid_oriented_meta.get(pmid, {}).copy()
        meta_parser(meta_doc)
        tmp_doc.update(meta_parser.meta_dict)
        docs.append(tmp_doc)
        if (i + 1) % 1000 == 0:
            print(f"loading {i+1} documents...")
    st = time.time()
    print(f"building index ...")
    ESIndex(index_name, docs)
    print(f"=== Built {index_name} in {round(time.time() - st, 2)} seconds ===")


if __name__ == "__main__":

    pmc_stmts_path = "raw_data/PPCA/statements_covid19-7-7.json"
    meta_input_path = "raw_data/sub_metadata-07-05.pkl"
    nlp = spacy.load("en_ner_bionlp13cg_md")
    ppi_parser = ParsePMCStmts.from_json(pmc_stmts_path, spacy_model=nlp)
    parser = argparse.ArgumentParser()
    parser.add_argument("index_name")
    args = parser.parse_args()
    load_es_index(args.index_name, ppi_parser, meta_input_path)
