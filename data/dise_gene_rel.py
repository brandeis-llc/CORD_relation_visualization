import pandas as pd
from data import load_pickled_obj


class ParseDiseGeneRel(object):
    """parse genes_diseases_relation.csv"""

    def __init__(self, input_file, gene_file, dise_file):
        print(f"building DiseGeneRel parser...")
        self.csv_df = pd.read_csv(input_file)
        self.gene_mapping = load_pickled_obj(gene_file)
        self.dise_mapping = load_pickled_obj(dise_file)

    def _quick_process(self, sub_df: pd.DataFrame):
        line_dict = sub_df.fillna("").to_dict()
        line_dict["GeneID"] = str(line_dict["GeneID"])
        line_dict["Gene"] = self.gene_mapping.get(str(line_dict["GeneID"]), None)
        line_dict["Disease"] = self.dise_mapping.get(line_dict["DiseaseID"], None)
        pmids_lst = line_dict["pmids"].split("|")
        del line_dict["pmids"]
        return line_dict, pmids_lst

    def __iter__(self):
        for line in self.csv_df.iloc:
            yield self._quick_process(line)


if __name__ == "__main__":
    gene_mapping_path = "../raw_data/genes_mapping.pkl"
    dise_mapping_path = "../raw_data/dis_mapping.pkl"
    chem_gen_rel_path = "../raw_data/KG/sub_genes_diseases_relation.csv"
    rel_parser = ParseDiseGeneRel(chem_gen_rel_path, gene_mapping_path, dise_mapping_path)
