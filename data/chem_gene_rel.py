from typing import Dict, List, Set, Tuple
import pandas as pd
from data import load_pickled_obj
from data.dise_gene_rel import ParseDiseGeneRel


class ParseChemGeneRel(object):
    """parse chem_gene_ixns_relation.csv"""

    def __init__(
        self, input_file, gene_file, chem_file, gene_dise_mapping: Dict[str, Set] = None
    ):
        print(f"building ChemGeneRel parser...")
        self.csv_df = pd.read_csv(input_file, delimiter="\t")
        self.ACTION_MAPPING = {"increases": "++", "decreases": "--", "affects": "->"}
        self.gene_mapping = load_pickled_obj(gene_file)
        self.chem_mapping = load_pickled_obj(chem_file)
        self.gene_dise_mapping = gene_dise_mapping
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
        self.CONTAINER_MAPPING = {
            "ADP-ribosylation": "ADP-ribosylator",
            "N-linked glycosylation": "N-linked glycosylator",
            "O-linked glycosylation": "O-linked glycosylator",
            "abundance": "Abundancer",
            "acetylation": "Acetylator",
            "activity": "Activator",
            "acylation": "Acylator",
            "alkylation": "Alkylator",
            "amination": "Aminator",
            "binding": "Binder",
            "carbamoylation": "Carbamoylator",
            "carboxylation": "Carboxylator",
            "chemical synthesis": "Chemical Synthesizer",
            "cleavage": "Cleavager",
            "cotreatment": "Cotreatment",
            "degradation": "Degradator",
            "ethylation": "Ethylator",
            "export": "Export",
            "expression": "Expression",
            "farnesylation": "Farnesylator",
            "folding": "Folder",
            "geranoylation": "Geranoylator",
            "glucuronidation": "Glucuronidator",
            "glutathionylation": "Glutathionylator",
            "glycation": "Glycator",
            "glycosylation": "Glycosylator",
            "hydrolysis": "Hydrolysizer",
            "hydroxylation": "Hydroxylator",
            "import": "Import",
            "lipidation": "Lipidator",
            "localization": "Localizer",
            "metabolic processing": "Metabolic Processor",
            "methylation": "Methylator",
            "mutagenesis": "Mutagenesizer",
            "myristoylation": "Myristoylator",
            "nitrosation": "Nitrosator",
            "nucleotidylation": "Nucleotidylator",
            "oxidation": "Oxidator",
            "palmitoylation": "Palmitoylator",
            "phosphorylation": "Kinase",
            "prenylation": "Prenylator",
            "reaction": "Regulator",
            "reduction": "Reducer",
            "response to substance": "Responsor",
            "ribosylation": "Ribosylator",
            "secretion": "Secret",
            "splicing": "Splicer",
            "stability": "Stabilizer",
            "sulfation": "Sulfator",
            "sumoylation": "Sumoylator",
            "transport": "Transport",
            "ubiquitination": "Ubiquitinator",
            "uptake": "Uptake",
        }

    def _parse_interaction_actions(
        self, interaction_str: str, target_gene: str, use_act_mapping=True
    ) -> Tuple[List[str], List[str]]:
        act_inter_pairs = interaction_str.split("|")
        container_lst = []
        for i, pair in enumerate(act_inter_pairs):
            pair = pair.split("^")
            if use_act_mapping:
                act_inter_pairs[i] = " ".join([self.ACTION_MAPPING[pair[0]], pair[1]])
                container_lst.append(
                    f"{self.ACTION_MAPPING[pair[0]]}{target_gene} {self.CONTAINER_MAPPING[pair[1]]}"
                )
            else:
                act_inter_pairs[i] = " ".join([pair[0], pair[1]])
        return act_inter_pairs, container_lst

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
            inter_action, containers = self._parse_interaction_actions(
                line_dict["InteractionActions"], line_dict["Gene"]["GeneSymbol"]
            )
            line_dict["InteractionActions"] = inter_action
            line_dict["Containers"] = containers
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
        inter_action, containers = self._parse_interaction_actions(
            line_dict["InteractionActions"], line_dict["Gene"]["GeneSymbol"]
        )
        line_dict["InteractionActions"] = inter_action
        line_dict["Containers"] = containers
        if self.gene_dise_mapping:
            dises = self.gene_dise_mapping.get(line_dict["GeneID"])
            line_dict["diseases"] = list(dises) if dises else None
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
    gene_dise_parser = ParseDiseGeneRel(
        "../raw_data/KG/sub_genes_diseases_relation.csv",
        gene_mapping_path,
        "../raw_data/dis_mapping.pkl",
    )
    rel_parser = ParseChemGeneRel(
        chem_gen_rel_path,
        gene_mapping_path,
        chem_mapping_path,
        gene_dise_parser.get_gene_dise_dist(),
    )
    for i, line in enumerate(rel_parser):
        print(line[0])
        if i > 10:
            break
