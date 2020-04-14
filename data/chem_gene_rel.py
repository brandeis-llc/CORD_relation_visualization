from typing import Dict, List
import pandas as pd
from data import load_pickled_obj


class ParseChemGeneRel(object):
    """parse chem_gene_ixns_relation.csv"""

    def __init__(self, input_file, gene_file, chem_file):
        print(f"building ChemGeneRel parser...")
        self.csv_df = pd.read_csv(input_file, delimiter="\t")
        self.ACTION_MAPPING = {"increases": "++", "decreases": "--", "affects": "->"}
        self.gene_mapping = load_pickled_obj(gene_file)
        self.chem_mapping = load_pickled_obj(chem_file)
        # TODO: overlapped ids are precomputed and hardcoded here
        self.overlapped_pmids = {
            "17288598",
            "17434272",
            "19409407",
            "19772353",
            "20003521",
            "23825585",
            "24244327",
            "24534281",
            "24561310",
            "25966753",
            "26551751",
            "26752173",
            "26917416",
            "27760801",
            "28438630",
            "28786057",
            "28806546",
            "29038090",
        }

    def _parse_interaction_actions(
        self, interaction_str: str, use_act_mapping=True
    ) -> List[str]:
        act_inter_pairs = interaction_str.split("|")
        for i, pair in enumerate(act_inter_pairs):
            pair = pair.split("^")
            if use_act_mapping:
                act_inter_pairs[i] = " ".join([self.ACTION_MAPPING[pair[0]], pair[1]])
            else:
                act_inter_pairs[i] = " ".join([pair[0], pair[1]])
        return act_inter_pairs

    def __call__(self, pmid: str) -> Dict[str, List]:
        if pmid not in self.overlapped_pmids:
            return {"action_interactions": []}
        sub_df = self.csv_df[self.csv_df.pmids == pmid]
        if not len(sub_df):
            return {"action_interactions": []}
        actions = []
        sub_df = sub_df.fillna("")
        for line in sub_df.iloc:
            line_dict = line.fillna("").to_dict()
            line_dict["GeneID"] = str(line_dict["GeneID"])
            line_dict["OrganismID"] = (
                str(int(line_dict["OrganismID"])) if line_dict["OrganismID"] else ""
            )
            line_dict["Gene"] = self.gene_mapping.get(str(line_dict["GeneID"]), "")
            line_dict["Chemical"] = self.chem_mapping.get(
                f"MESH:{line_dict['ChemicalID']}", ""
            )
            line_dict["InteractionActions"] = self._parse_interaction_actions(
                line_dict["InteractionActions"]
            )
            actions.append(line_dict)
        return {"action_interactions": actions}

    def _parse_interaction(self):
        # TODO: cleaning and more maybe
        raise NotImplementedError

    def _quick_process(self, sub_df: pd.DataFrame):
        line_dict = sub_df.fillna("").to_dict()
        line_dict["GeneID"] = str(line_dict["GeneID"])
        line_dict["OrganismID"] = (
            str(int(line_dict["OrganismID"])) if line_dict["OrganismID"] else ""
        )
        line_dict["Gene"] = self.gene_mapping.get(str(line_dict["GeneID"]), "")
        line_dict["Chemical"] = self.chem_mapping.get(
            f"MESH:{line_dict['ChemicalID']}", ""
        )
        line_dict["InteractionActions"] = self._parse_interaction_actions(
            line_dict["InteractionActions"]
        )
        pmids_lst = line_dict["pmids"].split("|")
        del line_dict["pmids"]
        return line_dict, pmids_lst

    def __iter__(self):
        for line in self.csv_df.iloc:
            yield self._quick_process(line)


if __name__ == "__main__":
    gene_mapping_path = "../raw_data/genes_mapping.pkl"
    chem_mapping_path = "../raw_data/chem_mapping.pkl"
    chem_gen_rel_path = "../raw_data/KG/chem_gene_ixns_relation.csv"
    rel_parser = ParseChemGeneRel(chem_gen_rel_path, gene_mapping_path, chem_mapping_path)
