from collections import namedtuple
from indra.tools import assemble_corpus as ac
from indra.statements.evidence import Evidence


class ParsePMCStmts(object):
    def __init__(self, input_file: str):
        """
        parse cord19_pmc_stmts_filt.pkl
        :param input_file:
        """
        self.stmts = ac.load_statements(input_file)
        self.relation = namedtuple("stmt", ["agents", "rel"])
        self.ALT_PMID = "UNK_PMID"
        self.Modification = "Modification"
        self.RegulateActivity = "RegulateActivity"
        self.Other = "Other"
        self.META_REL_TYPES = {
            "Acetylation": self.Modification,
            "Activation": self.RegulateActivity,
            "Autophosphorylation": self.Modification,
            "Complex": self.Other,
            "Deacetylation": self.Modification,
            "DecreaseAmount": self.RegulateActivity,
            "Deglycosylation": self.Modification,
            "Demethylation": self.Modification,
            "Depalmitoylation": self.Modification,
            "Dephosphorylation": self.Modification,
            "Deribosylation": self.Modification,
            "Desumoylation": self.Modification,
            "Deubiquitination": self.Modification,
            "Farnesylation": self.Modification,
            "Glycosylation": self.Modification,
            "Hydroxylation": self.Modification,
            "IncreaseAmount": self.RegulateActivity,
            "Inhibition": self.RegulateActivity,
            "Methylation": self.Modification,
            "Myristoylation": self.Modification,
            "Palmitoylation": self.Modification,
            "Phosphorylation": self.Modification,
            "Ribosylation": self.Modification,
            "Sumoylation": self.Modification,
            "Translocation": self.Other,
            "Ubiquitination": self.Modification,
        }

    @staticmethod
    def _get_rel_type(stmt) -> str:
        return type(stmt).__name__

    def __iter__(self):
        for stmt in self.stmts:
            agents = tuple(agent.name if agent else None for agent in stmt.agent_list())
            yield self.relation(agents, self._get_rel_type(stmt))

    def generate_evidence(self):
        for stmt in self.stmts:
            rel_type = self._get_rel_type(stmt)
            entities = [agent.name if agent else None for agent in stmt.agent_list()]
            meta_rel_type = self.META_REL_TYPES[rel_type]
            if meta_rel_type == self.Modification:
                KEY1 = "enz"
                KEY2 = "sub"
            elif meta_rel_type == self.RegulateActivity:
                KEY1 = "subj"
                KEY2 = "obj"
            else:
                KEY1 = "ent1"
                KEY2 = "ent2"
            for evi in stmt.evidence:
                pmid = evi.pmid if evi.pmid else self.ALT_PMID
                if len(entities) == 2:
                    yield pmid, {
                        KEY1: entities[0],
                        KEY2: entities[1],
                        "rel": rel_type,
                        "meta_rel": meta_rel_type,
                        "ents": entities,
                        "container": f"{rel_type}-{entities[1]}",
                    }
                elif entities:
                    yield pmid, {
                        KEY1: entities[0],
                        KEY2: None,
                        "rel": rel_type,
                        "meta_rel": meta_rel_type,
                        "ents": entities,
                    }
                else:
                    yield pmid, {
                        KEY1: None,
                        KEY2: None,
                        "rel": rel_type,
                        "meta_rel": meta_rel_type,
                        "ents": entities,
                    }

    def _parse_evidence(self, evidence: Evidence):
        # TODO: maybe we can retire this one
        try:
            pmid = evidence.pmid
            assert pmid is not None
        except AssertionError:
            pmid = self.ALT_PMID
        try:
            entities = evidence.annotations["agents"]["raw_text"]
        except AttributeError:
            entities = []
        return pmid, entities


if __name__ == "__main__":
    filename = "../raw_data/2020-03-20-john/cord19_pmc_stmts_filt.pkl"
    pps = ParsePMCStmts(filename)
    for i, rel in enumerate(pps.generate_evidence()):
        print(rel)
        if i > 100:
            break
