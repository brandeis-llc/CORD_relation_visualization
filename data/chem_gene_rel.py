from typing import Dict, List
from os import listdir, path
import json
import pandas as pd
from data import load_pickled_obj


class ParseChemGeneRel(object):
    """parse chem_gene_ixns_relation.csv"""
    def __init__(self, input_file, gene_file, chem_file):
        print(f'building ChemGeneRel parser...')
        self.csv_df = pd.read_csv(input_file, delimiter='\t')
        self.ACTION_MAPPING = {'increases': '++', 'decreases': '--', 'affects': '->'}
        self.gene_mapping = load_pickled_obj(gene_file)
        self.chem_mapping = load_pickled_obj(chem_file)

    def _parse_interaction_actions(self, interaction_str: str, use_act_mapping=True) -> List[str]:
        act_inter_pairs = interaction_str.split('|')
        for i, pair in enumerate(act_inter_pairs):
            pair = pair.split('^')
            if use_act_mapping:
                act_inter_pairs[i] = ' '.join([self.ACTION_MAPPING[pair[0]], pair[1]])
            else:
                act_inter_pairs[i] = ' '.join([pair[0], pair[1]])
        return act_inter_pairs

    def __call__(self, pmid: str) -> Dict[str, List]:
        sub_df = self.csv_df[self.csv_df.pmids == pmid]
        if not len(sub_df):
            return {'action_interactions': []}
        actions = []
        for line in sub_df.iloc:
            line_dict = line.fillna('').to_dict()
            line_dict['Gene'] = self.gene_mapping.get(str(line_dict['GeneID']), '')
            line_dict['Chemical'] = self.chem_mapping.get(line_dict['ChemicalID'], '')
            line_dict['InteractionActions'] = self._parse_interaction_actions(line_dict['InteractionActions'])
            actions.append(line_dict)
        return {'action_interactions': actions}

    def _parse_interaction(self):
        # TODO: cleaning and more maybe
        raise NotImplementedError


if __name__ == "__main__":
    gene_mapping_path = '../raw_data/genes_mapping.pkl'
    chem_mapping_path = '../raw_data/chem_mapping.pkl'
    chem_gen_rel_path = '../raw_data/KG/chem_gene_ixns_relation.csv'
    rel_parser = ParseChemGeneRel(chem_gen_rel_path, gene_mapping_path, chem_mapping_path)




