# csv_data.py
import uuid
import pandas as pd
from io import StringIO
from src.configuration.configuration import date_col

def dataframe_to_csv_string(df, date_column=date_col, row_index=1):
   
        # Retrieve the date from the DataFrame (adjust indexing as needed)
        actual_date = df.loc[row_index, date_column]
        
        # Ensure the date is in the correct format (if it's not already a string)
        if not isinstance(actual_date, str):
            actual_date_str = actual_date.strftime('%y-%m-%d')
        else:
            actual_date_str = actual_date

        # Create a StringIO object to hold the CSV data
        csv_buffer = StringIO()

        # Save the DataFrame to the StringIO object
        df.to_csv(csv_buffer, index=False)

        # Get the CSV data as a string
        csv_data = csv_buffer.getvalue()

        return csv_data, actual_date_str

    
def convert_dataframe_to_cosmos_format(df):
    cosmos_db_hartidata = []
    
    required_columns = ['Date', 'Category','Page', 'Pettah Average', 'Pettah_Min_Value', 'Pettah_Max_Value']
    
    # Check if all required columns are present
    if not all(col in df.columns for col in required_columns):
        raise KeyError(f"One or more required columns are missing: {required_columns}")
    
    for _, row in df.iterrows():
  
            # Assuming 'Date' column is in datetime format or can be parsed to datetime
            date_str = row['Date'].isoformat()  # Convert datetime to ISO 8601 format
            rate_document = {
                "id": str(uuid.uuid4()),
                "date": date_str,
                "category": row['Category'],
                "item": row['Item'],
                "page": row['Page'],
                "pettah_average": float(row['Pettah Average']) if pd.notna(row['Pettah Average']) else None,
                "pettah_min_value": float(row['Pettah_Min_Value']) if pd.notna(row['Pettah_Min_Value']) else None,
                "pettah_max_value": float(row['Pettah_Max_Value']) if pd.notna(row['Pettah_Max_Value']) else None,
            }
            cosmos_db_hartidata.append(rate_document)

    return cosmos_db_hartidata
