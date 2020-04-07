from os.path import join as pjoin
from data import pickle_obj_mapping
import pandas as pd
import argparse


def parse_genes_mapping(input_file: str, output_file: str):
    pickled = {}
    print(f'reading in {input_file}...')
    gene_df = pd.read_csv(input_file, delimiter='\t')
    gene_df.fillna('', inplace=True)
    print(f'writing pickled mapping to {output_file}...')
    for line in gene_df.iloc:
        pickled.update({str(line.GeneID): {'GeneName': line.GeneName, 'GeneSymbol': line.GeneSymbol}})
    pickle_obj_mapping(pickled, output_file)


def parse_chemicals_mapping(input_file: str, output_file: str):
    pickled = {}
    print(f'reading in {input_file}...')
    chem_df = pd.read_csv(input_file, delimiter='\t')
    chem_df.fillna('', inplace=True)
    print(f'writing pickled mapping to {output_file}...')
    for line in chem_df.iloc:
        pickled.update({str(line.ChemicalID): {'ChemicalName': line.ChemicalName}})
    pickle_obj_mapping(pickled, output_file)


def parse_diseases_mapping(input_file: str, output_file: str):
    pickled = {}
    print(f'reading in {input_file}...')
    dis_df = pd.read_csv(input_file, delimiter='\t')
    dis_df.fillna('', inplace=True)
    print(f'writing pickled mapping to {output_file}...')
    for line in dis_df.iloc:
        pickled.update({str(line.DiseaseID): {'DiseaseName': line.DiseaseName}})
    pickle_obj_mapping(pickled, output_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('input_dir')
    parser.add_argument('output_dir')
    args = parser.parse_args()
    parse_genes_mapping(pjoin(args.input_dir, 'genes.csv'), pjoin(args.output_dir, 'genes_mapping.pkl'))
    parse_chemicals_mapping(pjoin(args.input_dir, 'chemicals.csv'), pjoin(args.output_dir, 'chem_mapping.pkl'))
    parse_diseases_mapping(pjoin(args.input_dir, 'diseases.csv'), pjoin(args.output_dir, 'dis_mapping.pkl'))
