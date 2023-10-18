#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
import argparse 
from sqlalchemy import create_engine,text
from datetime import datetime




def extract_data(source):
    return pd.read_csv(source)

def transform_data(data):
    new_data = data.copy()
    new_data[['month', 'year']] = new_data.MonthYear.str.split(' ', expand=True)
    new_data['sex1'] = new_data['Sex upon Outcome'].replace('Unknown', np.nan)
    new_data[['sterilization', 'sex']] = new_data.sex1.str.split(' ', expand=True)
    

    #new_data[['ts','dummy']] = new_data.DateTime.str.split(' ',expand=True)
    #new_data[['month_outcome', 'year_outcome']] = new_data.date_of_outcome.str.split(' ', expand=True)
    new_data.drop(columns = ['MonthYear', 'Sex upon Outcome','sex1'], inplace=True)

    df = new_data.copy()
    
    df = df.rename(columns={
    "Animal ID": "animal_id",
    "Name": "animal_name",
    "DateTime": "ts",
    "Date of Birth": "dob",
    "Outcome Type": "outcome_type",
    "Outcome Subtype": "outcome_subtype",
    "Animal Type": "animal_type",
    #"Sex upon Outcome": "sex",
    "Age upon Outcome": "age",
    "Breed": "breed",
    "Color": "color"
    })

    #df['ts'] = pd.to_datetime(df['ts'])
    df['dob'] = pd.to_datetime(df['dob'])
    #df['year'] = df['dob'].dt.year
    #df['month'] = df['dob'].dt.month
    #df['age'] = df['age'].str.extract('(\d+)').astype(int)
    df['year'] = df['year'].astype(int)

    return df

def load_data(data):
    db_url = "postgresql+psycopg2://annanya:annanya@db:5432/shelter"
    conn = create_engine(db_url)
    data.to_sql("outcomes",conn, if_exists = "append", index= False)

    animal_dim = text("""
    INSERT INTO animal_dim (animal_id, animal_name, breed, color, dob, outcome_subtype)
    SELECT DISTINCT animal_id, animal_name, breed, color, dob, outcome_subtype
    FROM outcomes;
    """)
    with conn.begin() as connection:
        connection.execute(animal_dim)

        # Create Date Dimension
    date_dim = text("""
    INSERT INTO  date_dim (
       ts,
       month,
       year
    )

    SELECT DISTINCT ts, month, year
    FROM outcomes;
    """)
    with conn.begin() as connection:
        connection.execute(date_dim)

    # Create Type Dimension
    type_dim = text("""
    INSERT INTO type_dim (animal_type)
    SELECT DISTINCT animal_type
    FROM outcomes;
    """)
    with conn.begin() as connection:
        connection.execute(type_dim)

    outcome_dim = text("""
    INSERT INTO outcome_dim (outcome_type)
    SELECT DISTINCT outcome_type
    FROM outcomes;
    """)
    with conn.begin() as connection:
        connection.execute(outcome_dim)


    # Create Sex Dimension
    sex_dim = text("""
    INSERT INTO sex_dim (
       sex ,
       sterilization
    )
    SELECT DISTINCT sex, sterilization
    FROM outcomes;
    """)
    with conn.begin() as connection:
        connection.execute(sex_dim)


    outcomes_fact = text("""
    INSERT INTO outcomes_fact (animal_dim_key, date_id, outcome_id, type_id, sex_id)
    SELECT ad.animal_dim_key, dd.date_id, od.outcome_id, td.type_id, sd.sex_id
    FROM outcomes o
    JOIN Animal_dim ad ON o.animal_id = ad.animal_id
    JOIN Date_dim dd ON o.ts = dd.ts
    JOIN Outcome_dim od ON o.outcome_type = od.outcome_type
    JOIN Type_dim td ON o.animal_type = td.animal_type
    JOIN Sex_dim sd ON o.sex = sd.sex;  -- Modify this join condition if needed
    """)
    with conn.begin() as connection:
        connection.execute(outcomes_fact)

if __name__ == "__main__": 
    parser = argparse.ArgumentParser()
    parser.add_argument('source', help='source csv')
    #parser.add_argument('target', help='target csv')
    args = parser.parse_args()

    print("Starting...")
    df = extract_data(args.source)
    new_df = transform_data(df)
    load_data(new_df)
    print("Complete")