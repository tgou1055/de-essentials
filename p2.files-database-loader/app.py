"""
files-database-loader
"""

import sys
import os
import glob
import json
import re
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

def get_column_names(schemas: dict, ds_name: str, sorting_key='column_position'):
    """
    Get column names from schemas.json
    """
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda col: col[sorting_key], reverse=False)
    return [col['column_name'] for col in columns]


def read_csv(file, schemas, chunksize=30000):
    """
    Read csv file by using the column names in schema
    """
    ds_name = re.split('[/]', file)[-2]
    columns = get_column_names(schemas, ds_name)
    df = pd.read_csv(file, names=columns, chunksize=chunksize) # our server spec is limitted
    #df = pd.read_csv(file, names=columns)
    return df

def to_sql(df, db_conn_uri, ds_name, ds_schema):
    """
    Load df to database table
    """
    df.to_sql(
        name=ds_name,
        con=db_conn_uri,
        schema=ds_schema,
        if_exists='append',
        index=False
    )

def db_loader(src_base_dir, db_conn_uri, ds_name, ds_schema, schemas):
    """
    File converter function
    """
    files = glob.glob(f'{src_base_dir}/{ds_name}/part-*')
    if len(files) == 0:
        raise NameError(f'No files found for {ds_name}')

    for file in files:
        df_list = list(read_csv(file, schemas))
        #df_list = read_csv(file, schemas)
        for index, df in enumerate(df_list):
            print(f'Processing {ds_name}: Size of chunk {index} is {df.shape}')
            to_sql(df, db_conn_uri, ds_name, ds_schema)

def process_files(ds_names=None):
    """
    Processing files
    """
    src_base_dir = os.getenv('SRC_BASE_DIR')
    db_host = os.getenv('POSTGRES_HOST')
    db_port = os.getenv('POSTGRES_PORT')
    db_usr = os.getenv('POSTGRES_USR')
    db_pwd = os.getenv('POSTGRES_PWD')
    db_db = os.getenv('POSTGRES_DB')
    ds_schema= os.getenv('POSTGRES_SCHEMA')
    db_conn_uri = f'postgresql://{db_usr}:{db_pwd}@{db_host}:{db_port}/{db_db}'
    with open(f'{src_base_dir}/schemas.json', encoding='utf-8') as schemas_json:
        schemas = json.load(schemas_json)

    if not ds_names:
        ds_names = schemas.keys()
    for ds_name in ds_names:
        try:
            print(f'Processing {ds_name}')
            db_loader(src_base_dir, db_conn_uri, ds_name, ds_schema, schemas)
        except NameError as ne:
            print(ne)
            print(f'Error processing {ds_name}')
            pass
        except Exception as e:
            print(e)
            pass
        finally:
            print(f'Complete Processing {ds_name}')

if __name__ == '__main__':
    # use run time arguments to pass data file names
    if len(sys.argv) == 2:
        t_names = json.loads(sys.argv[1])
        process_files(t_names)
    else:
        process_files()
