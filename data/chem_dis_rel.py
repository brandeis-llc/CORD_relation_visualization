import pandas as pd
from data import load_pickled_obj


class ParseDiseChemRel(object):
    """parse genes_diseases_relation.csv"""

    def __init__(self, input_file, dise_file, chem_file):
        print(f"building DiseChemRel parser...")
        self.csv_df = pd.read_csv(input_file)
        self.dise_mapping = load_pickled_obj(dise_file)
        self.chem_mapping = load_pickled_obj(chem_file)

    def _quick_process(self, sub_df: pd.DataFrame):
        line_dict = sub_df.fillna("").to_dict()
        line_dict["Disease"] = self.dise_mapping.get(line_dict["DiseaseID"], None)
        line_dict["Chemical"] = self.chem_mapping.get(
            f"MESH:{line_dict['ChemicalID']}", None
        )
        pmids_lst = line_dict["pmids"].split("|")
        del line_dict["pmids"]
        return line_dict, pmids_lst

    def __iter__(self):
        for line in self.csv_df.iloc:
            yield self._quick_process(line)


if __name__ == "__main__":
    chem_mapping_path = "../raw_data/chem_mapping.pkl"
    dise_mapping_path = "../raw_data/dis_mapping.pkl"
    chem_gen_rel_path = "../raw_data/KG/sub_chemicals_diseases_relation.csv"
    rel_parser = ParseDiseChemRel(chem_gen_rel_path, dise_mapping_path, chem_mapping_path)
    for l in rel_parser:
        print(l)
