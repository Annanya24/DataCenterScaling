import pandas as pd
import numpy as np
from pathlib import Path

# Creating the global mapping for outcome types
outcomes_map = {'Rto-Adopt': 1,
                'Adoption': 2,
                'Euthanasia': 3,
                'Transfer': 4,
                'Return to Owner': 5,
                'Died': 6,
                'Disposal': 7,
                'Missing': 8,
                'Relocate': 9,
                'N/A': 10,
                'Stolen': 11}

def transform_data(source_csv, target_dir):
    
    new_data = pd.read_csv(source_csv)
    new_data = prep_data(new_data)

    dim_animal = prep_animal_dim(new_data)
    dim_dates = prep_date_dim(new_data)
    dim_outcome_types = prep_outcome_types_dim(new_data)
    dim_sex = prep_dim_sex(new_data)
    dim_type = prep_type_dim(new_data)

    fct_outcomes = prep_outcomes_fct(new_data)

    Path(target_dir).mkdir(parents=True, exist_ok=True)

    # Saving dimensions to parquet files
    dim_animal.to_parquet(target_dir+'/dim_animals.parquet')
    dim_dates.to_parquet(target_dir+'/dim_dates.parquet')
    dim_outcome_types.to_parquet(target_dir+'/dim_outcome_types.parquet')
    dim_sex.to_parquet(target_dir+'/dim_sex.parquet')
    dim_type.to_parquet(target_dir+'/dim_type.parquet')

    # Saving fact table to parquet file
    fct_outcomes.to_parquet(target_dir+'/fct_outcomes.parquet')

def prep_data(data):
    print("Column names:", data.columns)
    # remove stars from animal names. Need regex=False so that * isn't read as regex

    # for some reason after using API my column names were already in proper format so i changed them to wrong format 
    #cause the rest of the code is based on capitalized format
    column_mapping = {
    'animal_id': 'Animal ID',
    'name': 'Name',
    'datetime': 'DateTime',
    'monthyear': 'MonthYear',
    'date_of_birth': 'Date of Birth',
    'outcome_type': 'Outcome Type',
    'outcome_subtype': 'Outcome Subtype',
    'animal_type': 'Animal Type',
    'sex_upon_outcome': 'Sex upon Outcome',
    'age_upon_outcome': 'Age upon Outcome',
    'breed': 'Breed',
    'color': 'Color'
    }

    # Rename columns if they exist in the DataFrame
    for old_name, new_name in column_mapping.items():
        if old_name in data.columns:
            data = data.rename(columns={old_name: new_name})


    data['name'] = data['Name'].str.replace("*", "", regex=False)

    # separate the "sex upon outcome" column into property of an animal (male or female)
    # and property of an outcome (was the animal spayed/neutered at the shelter or not)
    data['sex'] = data['Sex upon Outcome'].replace({"Neutered Male": "M",
                                                    "Intact Male": "M",
                                                    "Intact Female": "F",
                                                    "Spayed Female": "F",
                                                    "Unknown": np.nan})

    data['sterilization'] = data['Sex upon Outcome'].replace({"Neutered Male": True,
                                                         "Intact Male": False,
                                                         "Intact Female": False,
                                                         "Spayed Female": True,
                                                         "Unknown": np.nan})

    # prepare the data table for introducing the date dimension
    # we'll use condensed date as the key, e.g. '20231021'
    # time can be a separate dimension, but here we'll keep it as a field
    data['ts'] = pd.to_datetime(data.DateTime)
    data['date_id'] = data.ts.dt.strftime('%Y%m%d')
    data['time'] = data.ts.dt.time

    # prepare the data table for introducing the outcome type dimension:
    # introduce keys for the outcomes
    data['outcome_type_id'] = data['Outcome Type'].fillna('N/A')
    data['outcome_type_id'] = data['outcome_type_id'].replace(outcomes_map)

    data['type_id'] = pd.Categorical(data['Animal Type']).codes + 1
    data['sex_id'] = pd.Categorical(data['sex']).codes + 1

    return data

def prep_animal_dim(data):
    
    # extract columns only relevant to animal dim
    animal_dim = data[['Animal ID','name','Date of Birth', 'Breed', 'Color']]
    
    # rename the columns to agree with the DB tables
    animal_dim.columns = ['animal_id', 'name', 'dob', 'breed', 'color']
    
    # drop duplicate animal records
    return animal_dim.drop_duplicates()


def prep_date_dim(data):
    # use string representation as a key
    # separate out year, month, and day
    dates_dim = pd.DataFrame({
        'date_id':data.ts.dt.strftime('%Y%m%d'),
        'date':data.ts.dt.date,
        'year':data.ts.dt.year,
        'month':data.ts.dt.month,
        'day':data.ts.dt.day,
        })
    return dates_dim.drop_duplicates()

def prep_outcome_types_dim(data):
    # map outcome string values to keys
    outcome_types_dim = pd.DataFrame.from_dict(outcomes_map, orient='index').reset_index()
    
    # keep only the necessary fields
    outcome_types_dim.columns=['outcome_type', 'outcome_type_id']  
    return outcome_types_dim

def prep_dim_sex(data):
    # Extract unique values for 'sex' and 'sterilization'
    unique_sex_values = data['sex'].unique()
    unique_sterilization_values = data['sterilization'].unique()

    # Create a DataFrame with the unique values and assign IDs
    dim_sex = pd.DataFrame({
        'sex': unique_sex_values,
        'sterilization': unique_sterilization_values,
        'sex_id': range(1, len(unique_sex_values) + 1)
    })

    return dim_sex


def prep_type_dim(data):
    # Create a type_dim based on unique values in 'animal_type' column
    dim_type = pd.DataFrame({
        'type_id': range(1, len(data['Animal Type'].unique()) + 1),
        'animal_type': data['Animal Type'].unique(),
    })
    return dim_type

def prep_outcomes_fct(data):
    # pick the necessary columns and rename
    outcomes_fct = data[['Animal ID', 'date_id', 'time', 'outcome_type_id', 'type_id', 'sex_id','Outcome Subtype']]
    return outcomes_fct.rename(columns={"Animal ID": "animal_id", "Outcome Subtype":"outcome_subtype"})

