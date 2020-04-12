import argparse
import pickle
from typing import Sequence
import pandas as pd


def get_sub_meta_csv(in_file: str, fields: Sequence, allow_empty: bool, out_file: str, to_pickle: bool):
    """
    generate a new covid meta csv based on the given fields
    :param in_file: original meta csv
    :param fields: a list of fields you want to keep
    :param allow_empty: allow empty values (N/A)
    :param out_file: output file path
    :param to_pickle: write the output dict into a pickle
    :return:
    """
    csv_df = pd.read_csv(in_file)
    columns = csv_df.columns.values
    invalid_fields = set(fields) - set(columns)
    if invalid_fields:
        print(f'Ignore fields: {invalid_fields}')
        fields = set(fields) - invalid_fields
    if fields:
        csv_df = csv_df[fields]
    if not allow_empty:
        # TODO: sha and pmid are not empty so that we can trace back to the original paper and parsed json doc
        csv_df = csv_df.dropna(subset=['pubmed_id']).reset_index(drop=True)
    csv_df.to_csv(out_file, index=False)
    print(f'writing to {out_file}...')
    if to_pickle:
        try:
            csv_df = csv_df.astype({'pubmed_id': 'int32'}).astype({'pubmed_id': 'str'})
        except:
            pass
        csv_df = csv_df.fillna('')
        csv_dict_lst = csv_df.to_dict('records')
        pkl_name = out_file.split('.')[0] + '.pkl'
        with open(f"{pkl_name}", 'wb') as f:
            pickle.dump(csv_dict_lst, f)
        print(f'writing to {pkl_name}...')


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--allow_empty', '-e', action='store_true')
    parser.add_argument('in_path')
    parser.add_argument('out_path')
    parser.add_argument('fields', nargs='+')
    parser.add_argument('--to_pkl', '-p', action='store_true')

    args = parser.parse_args()

    get_sub_meta_csv(args.in_path, args.fields, args.allow_empty, args.out_path, args.to_pkl)
