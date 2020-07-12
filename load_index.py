import time
import attr
import argparse
from typing import List, Dict
from collections import defaultdict

from elastic_index import ESIndex
import spacy

from data.parse_pmc_stats import ParsePMCStmts
from data.meta import ParseMetaData
from data import pickle_obj_mapping, load_pickled_obj


@attr.s(auto_attribs=True)
class IndexLoader:
    index_name: str = attr.ib()
    docs: List[Dict] = attr.ib()

    def load(self):
        st = time.time()
        print(f"building index ...")
        ESIndex(self.index_name, self.docs)
        print(f"=== Built {self.index_name} in {round(time.time() - st, 2)} seconds ===")

    @staticmethod
    def _get_meta_docs(meta_pkl="raw_data/sub_metadata.pkl"):
        meta_data: List[Dict] = load_pickled_obj(meta_pkl)
        pmid_oriented_meta = {item["pubmed_id"]: item for item in meta_data}
        return pmid_oriented_meta

    @classmethod
    def from_pmc_stats(cls, index_name, source_file_path: str, docs_pkl: str):
        try:
            docs = load_pickled_obj(docs_pkl)
        except FileNotFoundError:
            docs = []
            nlp = spacy.load("en_ner_bionlp13cg_md")
            if source_file_path.endswith(".json"):
                pmc_stats_parser = ParsePMCStmts.from_json(
                    source_file_path, spacy_model=nlp
                )
            elif source_file_path.endswith(".pkl"):
                pmc_stats_parser = ParsePMCStmts.from_pkl(
                    source_file_path, spacy_model=nlp
                )
            else:
                raise TypeError(f"Cannot identify file {source_file_path}!")
            meta_docs = cls._get_meta_docs()
            meta_parser = ParseMetaData()
            for i, (pmid, ppi_doc) in enumerate(
                pmc_stats_parser.generate_evidence_dict()
            ):
                tmp_doc = {
                    "pubmed_id": pmid,
                    "PPIs": ppi_doc,
                    "doc_id": pmid + "-" + str(i),
                }
                meta_doc = meta_docs.get(pmid, {}).copy()
                meta_parser(meta_doc)
                tmp_doc.update(meta_parser.meta_dict)
                docs.append(tmp_doc)
                if (i + 1) % 10000 == 0:
                    print(f"loading {i + 1} documents...")
                pickle_obj_mapping(docs, docs_pkl)
            print(f"Writing docs to {docs_pkl}...")
        return IndexLoader(index_name, docs)
