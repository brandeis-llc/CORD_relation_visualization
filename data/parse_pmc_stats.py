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
        self.relation = namedtuple('stmt', ['ent1', 'ent2', 'rel'])
        self.ALT_PMID = 'UNK_PMID'

    @staticmethod
    def _get_rel_type(stmt):
        return type(stmt).__name__

    def __iter__(self):
        for stmt in self.stmts:
            agents = stmt.agent_list()
            yield self.relation(agents[0], agents[1], self._get_rel_type(stmt))

    def generate_evidence(self):
        for stmt in self.stmts:
            rel_type = self._get_rel_type(stmt)
            for evi in stmt.evidence:
                pmid, entities = self._parse_evidence(evi)
                if len(entities) == 2:
                    yield pmid, {'ent1': entities[0], 'ent2': entities[1], 'rel': rel_type}
                elif entities:
                    yield pmid, {'ent1': entities[0], 'ent2': None, 'rel': rel_type}
                else:
                    yield pmid, {'ent1': None, 'ent2': None, 'rel': rel_type}

    def _parse_evidence(self, evidence: Evidence):
        try:
            pmid = evidence.pmid
            assert pmid is not None
        except AssertionError:
            pmid = self.ALT_PMID
        try:
            entities = evidence.annotations['agents']['raw_text']
        except AttributeError:
            entities = []
        return pmid, entities,


if __name__ == '__main__':
    filename = '../raw_data/2020-03-20-john/cord19_pmc_stmts_filt.pkl'
    pps = ParsePMCStmts(filename)
    for i, rel in enumerate(pps.generate_evidence()):
        print(rel)
        if i > 100:
            break




