"""
Test: File Format Convert Application 

    - The last @patch decorator applied is the first mock argument 
      passed to the test function.
"""

import os
import json
from unittest.mock import patch, mock_open, ANY
import pytest
import pandas as pd

from app import (get_column_names, read_csv, to_json, file_converter, process_files)

@pytest.fixture
def dummy_schema():
    """
    A dummy schmea
    """
    return {"customers": [
                {"column_name": "customer_id", "data_type": "integer", "column_position": 1},
                {"column_name": "customer_fname","data_type": "string","column_position": 2},
                {"column_name": "customer_lname","data_type": "string","column_position": 3},
                {"column_name": "customer_email","data_type": "string","column_position": 4}
            ]
    }

@pytest.fixture
def dummy_csv():
    """
    A dummy csv file for dummy schema
    """
    return "5,Robert,Hudson,robert.hudson1985@gmail.com"

def test_get_column_names(dummy_schema): #pylint ignore: redefined-outer-name
    """
    Test get_column_names:
        1. test column extraction and sorting
        2. test with a non-existing dataset in schema
    """
    #1.
    result = get_column_names(dummy_schema, 'customers')
    assert result == ["customer_id", "customer_fname", "customer_lname", "customer_email"]

    #2.
    with pytest.raises(KeyError):
        get_column_names(dummy_schema, 'non_existing_dummy')


@patch('pandas.read_csv')
def test_read_csv(mock_read_csv, dummy_schema):
    """
    Test read_csv:
        1. mock the pandas read_csv method
        2. check if the columns match the schema
    """
    #1
    mock_df = pd.DataFrame({"customer_id": [5],
                            "customer_fname":["Robert"], 
                            "customer_lname":["Hudson"], 
                            "customer_email":["robert.hudson1985@gmail.com"]
                            })
    mock_read_csv.return_value = mock_df

    df = read_csv('path/customers/part-0000*', dummy_schema)

    #2
    assert list(df.columns) == ["customer_id","customer_fname","customer_lname","customer_email"]
    mock_read_csv.assert_called_once()

@patch('pandas.DataFrame.to_json')
@patch('os.makedirs')
def test_to_json(mock_makedirs, mock_to_json):
    """
    Test to_json:
        1. mock pandas dataframe and directory creation
        2. check os.makedirs is called
    """

    #1.
    mock_df = pd.DataFrame({"customer_id": [5],
                            "customer_fname":["Robert"], 
                            "customer_lname":["Hudson"], 
                            "customer_email":["robert.hudson1985@gmail.com"]
                            })
    to_json(mock_df, "trg_dir", "customers", "part-0000*")

    #2.
    mock_makedirs.assert_called_once_with("trg_dir/customers", exist_ok=True)
    mock_to_json.assert_called_once()

@patch('glob.glob')
@patch('app.read_csv')
@patch('app.to_json')
def test_file_converter(mock_to_json, mock_read_csv, mock_glob, dummy_schema):
    """
    Test file_converter:
        1. Mock glob
        2. Mock Dataframe as read_csv return value
        3. Run file_converter and assert calls
    """
    #1.
    mock_glob.return_value = ["src_dir/customers/part-00000", "src_dir/customers/part-00001"]

    #2.
    mock_read_csv.return_value = pd.DataFrame({"customer_id": [5],
                            "customer_fname":["Robert"], 
                            "customer_lname":["Hudson"], 
                            "customer_email":["robert.hudson1985@gmail.com"]
                            })
    #3.
    file_converter("src_dir", "trg_dir", dummy_schema, "customers")

    mock_glob.assert_called_once_with('src_dir/customers/part-*')
    assert mock_read_csv.call_count == 2
    assert mock_to_json.call_count == 2

@patch.dict(os.environ, {'SRC_BASE_DIR': 'src_dir', 'TRG_BASE_DIR': 'trg_dir'})
@patch('builtins.open',
       new_callable=mock_open,
       read_data='{"customers": [{"column_name": "id", "column_position": 1}]}')
@patch('app.file_converter')
def test_process_files(mock_file_converter, mock_open_file):
    """
    Test process_files:
        1. Test process_files with specific dataset names
        2. Assert that the schemas file was opened
        3. Assert file_converter was called
    """
    #1.
    process_files(['customers'])

    #2.
    mock_open_file.assert_called_once_with('src_dir/schemas.json', encoding='utf-8')

    #3.
    mock_file_converter.assert_called_once_with('src_dir', 'trg_dir',
                                                json.load(mock_open_file()), 'customers')

@patch.dict(os.environ, {'SRC_BASE_DIR': 'src_dir', 'TRG_BASE_DIR': 'trg_dir'})
@patch('builtins.open',
       new_callable=mock_open,
       read_data='{"customers": [{"column_name": "id", "column_position": 1}]}')
@patch('app.file_converter', side_effect=NameError("No files found for cutsomers"))
def test_process_files_name_error(mock_file_converter, mock_open_file):
    """
    Test process_files_name_error
    """
    # Call process_files to trigger the exception
    process_files(['cutsomers'])

    # Check that the error message was printed
    mock_open_file.assert_called_once_with('src_dir/schemas.json', encoding='utf-8')

    mock_file_converter.assert_called_once_with('src_dir', 'trg_dir',
                                                json.load(mock_open_file()), 'cutsomers')

    with pytest.raises(NameError, match="No files found for cutsomers"):
        file_converter('src_dir', 'trg_dir', json.load(mock_open_file()), 'cutsomers')
