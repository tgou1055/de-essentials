"""
File Format Convert Application

    - use wrapper function process_files()
    - use environment variables
    - use impact analysis to handle false input of file names
      - raise NameError
      - except NameError -> pass

    - to run
      python3 app.py '["customers", "departments", ..]'
"""

import sys
import os
import glob
import json
import re
import pandas as pd
from dotenv import load_dotenv


def get_column_names(schemas: dict, ds_name: str, sorting_key='column_position'):
    """
    Get column names from schemas.json
    """
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key], reverse=False)
    return [col['column_name'] for col in columns]

def read_csv(file, schemas):
    """
    Read csv file by using the column names in schema
    """
    ds_name = re.split('[/]', file)[-2]
    columns = get_column_names(schemas, ds_name)
    df = pd.read_csv(file, names=columns)
    return df

def to_json(df, trg_base_dir, ds_name, file_name):
    """
    Convert csv file to json file using pandas dataframe
    """
    json_file_path = f'{trg_base_dir}/{ds_name}/{file_name}'
    os.makedirs(f'{trg_base_dir}/{ds_name}', exist_ok=True)
    df.to_json(
        json_file_path,
        orient='records',
        lines=True
    )

def file_converter(src_base_dir, trg_base_dir, schemas, ds_name):
    """
    File converter function
    """
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')
    if len(files) == 0:
        raise NameError(f'No files found for {ds_name}')

    for file in files:
        df = read_csv(file, schemas)
        file_name = re.split('[/]', file)[-1]
        to_json(df, trg_base_dir, ds_name, file_name)

def process_files(ds_names=None):
    """
    Wrapper function
    """
    src_base_dir = os.environ.get('SRC_BASE_DIR')
    trg_base_dir = os.environ.get('TRG_BASE_DIR')
    with open(f'{src_base_dir}/schemas.json', encoding='utf-8') as schemas_json:
        schemas = json.load(schemas_json)
    if not ds_names:
        ds_names = schemas.keys()
    for ds_name in ds_names:
        try:
            print(f'Processing {ds_name}')
            file_converter(src_base_dir, trg_base_dir, schemas, ds_name)
        except NameError as ne:
            print(ne)
            print(f'Error processing {ds_name}')
            pass


if __name__ == '__main__':
    # Load environment variables from the .env file
    load_dotenv()
    # use run time arguments to pass data file names
    if len(sys.argv) == 2:
        t_names = json.loads(sys.argv[1])
        process_files(t_names)
    else:
        process_files()
