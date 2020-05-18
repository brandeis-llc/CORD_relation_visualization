import attr
from typing import Tuple, Optional, List, Any
from collections import defaultdict
from indra.tools import assemble_corpus as ac
from indra.statements.evidence import Evidence
from indra.statements import Statement
import spacy


@attr.s()
class Relation:
    agents: Tuple[Optional[str], ...] = attr.ib()
    rel: str = attr.ib()


@attr.s(auto_attribs=True)
class ParsePMCStmts:
    stmts: List["Statement"] = attr.ib()
    spacy_model: Any = attr.ib(None)
    UNK_PMID: str = attr.ib("UNK_PMID")
    Modification: str = "Modification"
    RegulateActivity: str = "RegulateActivity"
    Other: str = "Other"
    META_REL_TYPES = {
        "Acetylation": Modification,
        "Activation": RegulateActivity,
        "Autophosphorylation": Modification,
        "Complex": Other,
        "Deacetylation": Modification,
        "DecreaseAmount": RegulateActivity,
        "Deglycosylation": Modification,
        "Demethylation": Modification,
        "Depalmitoylation": Modification,
        "Dephosphorylation": Modification,
        "Deribosylation": Modification,
        "Desumoylation": Modification,
        "Deubiquitination": Modification,
        "Farnesylation": Modification,
        "Glycosylation": Modification,
        "Hydroxylation": Modification,
        "IncreaseAmount": RegulateActivity,
        "Inhibition": RegulateActivity,
        "Methylation": Modification,
        "Myristoylation": Modification,
        "Palmitoylation": Modification,
        "Phosphorylation": Modification,
        "Ribosylation": Modification,
        "Sumoylation": Modification,
        "Translocation": Other,
        "Ubiquitination": Modification,
    }

    @classmethod
    def from_pkl(
        cls, input_pkl: str, *, spacy_model=None, unk_pmid="UNK_PMID"
    ) -> "ParsePMCStmts":
        stmts = ac.load_statements(input_pkl)
        return ParsePMCStmts(stmts, spacy_model, unk_pmid)

    @staticmethod
    def _get_rel_type(stmt: "Statement") -> str:
        return type(stmt).__name__

    def _get_agent_types(self, meta_rel_type: str) -> Tuple[str, str]:
        if meta_rel_type == self.Modification:
            KEY1 = "enz"
            KEY2 = "sub"
        elif meta_rel_type == self.RegulateActivity:
            KEY1 = "subj"
            KEY2 = "obj"
        else:
            KEY1 = "ent1"
            KEY2 = "ent2"
        return KEY1, KEY2

    @staticmethod
    def _get_container_name(rel_type: str) -> str:
        if rel_type.endswith("tion"):
            return rel_type[:-3] + "or"
        elif rel_type.endswith("Amount"):
            return rel_type[:-6] + "er"
        else:
            return rel_type

    def _get_ents(self, text: str):
        doc = self.spacy_model(text)
        ents = doc.ents
        ents_dict = defaultdict(list)
        for ent in ents:
            ents_dict[ent.label_].append(ent.text)
            ents_dict["all"].append(ent.text)
        return ents_dict

    def __iter__(self):
        for stmt in self.stmts:
            agents = tuple(agent.name if agent else None for agent in stmt.agent_list())
            yield Relation(agents, self._get_rel_type(stmt))

    def generate_evidence(self):
        for stmt in self.stmts:
            rel_type = self._get_rel_type(stmt)
            entities = [agent.name if agent else None for agent in stmt.agent_list()]
            meta_rel_type = self.META_REL_TYPES[rel_type]
            KEY1, KEY2 = self._get_agent_types(meta_rel_type)
            for evi in stmt.evidence:
                if evi.pmid:
                    pmid = evi.pmid
                    pmid_url = f"https://www.ncbi.nlm.nih.gov/pubmed/{pmid}"
                else:
                    pmid = self.UNK_PMID
                    pmid_url = None
                evi_ents = self._get_ents(evi.text)

                if len(entities) == 2:
                    yield pmid, {
                        KEY1: entities[0],
                        KEY2: entities[1],
                        "rel": rel_type,
                        "meta_rel": meta_rel_type,
                        "ents": entities,
                        "container": f"{entities[1]} {self._get_container_name(rel_type)}",
                        "text": evi.text,
                        "evi_ents": evi_ents,
                        "pmid_url": pmid_url,
                    }
                elif entities:
                    yield pmid, {
                        KEY1: entities[0],
                        KEY2: None,
                        "rel": rel_type,
                        "meta_rel": meta_rel_type,
                        "ents": entities,
                        "text": evi.text,
                        "evi_ents": evi_ents,
                        "pmid_url": pmid_url,
                    }
                else:
                    yield pmid, {
                        KEY1: None,
                        KEY2: None,
                        "rel": rel_type,
                        "meta_rel": meta_rel_type,
                        "ents": entities,
                        "text": evi.text,
                        "evi_ents": evi_ents,
                        "pmid_url": pmid_url,
                    }

    def _parse_evidence(self, evidence: Evidence):
        # TODO: maybe we can retire this one
        try:
            pmid = evidence.pmid
            assert pmid is not None
        except AssertionError:
            pmid = self.UNK_PMID
        try:
            entities = evidence.annotations["agents"]["raw_text"]
        except AttributeError:
            entities = []
        return pmid, entities


if __name__ == "__main__":
    filename = "../raw_data/2020-03-20-john/cord19_pmc_stmts_filt.pkl"
    nlp = spacy.load("en_ner_bionlp13cg_md")
    pps = ParsePMCStmts.from_pkl(filename, spacy_model=nlp)
    for i, rel in enumerate(pps.generate_evidence()):
        if i % 1000 == 0:
            print(rel)
            break
