import pandas as pd
from datetime import datetime

def add_database_write_date(df):

    df['Database Write Date'] = datetime.now().strftime('%Y-%m-%d')
    return df

def reorder_columns(df):

    cols = ['Database Write Date'] + [col for col in df if col != 'Database Write Date']
    df = df[cols]
    return df

def convert_date_column(df):
    possible_date_formats = {
        '%Y.%m.%d', # 2024.02.24
        '%d.%m.%Y', # 24.02.2024
        '%Y-%m-%d', # 2024-02-24
        '%d-%m-%Y', # 24-02-2024
        '%Y/%m/%d', # 2024/02/24
        '%d/%m/%Y'  # 24/02/2024
    }

    for possible_date_format in possible_date_formats:
        try: 
            df['Date'] = pd.to_datetime(df['Date'], format=possible_date_format)
            break
        except ValueError:
            continue
    
    return df

def split_value_column(df):

    df[['Pettah_Min_Value', 'Pettah_Max_Value']] = df['Pettah Price Range'].str.split(' - ', expand=True)
    
    # Convert the new columns to integers, handling potential conversion errors
    df['Pettah_Min_Value'] = pd.to_numeric(df['Pettah_Min_Value'], errors='coerce').fillna(0).astype(int)
    df['Pettah_Max_Value'] = pd.to_numeric(df['Pettah_Max_Value'], errors='coerce').fillna(0).astype(int)
    
    df = df.drop(columns=['Pettah Price Range'])

    return df

def add_page_number(df):

    df['Page'] = 1

    return df

def transform_dataframe(df):

    df = add_database_write_date(df)
    df = reorder_columns(df)
    df = convert_date_column(df)
    df = split_value_column(df)
    df = add_page_number(df)
    
    return df