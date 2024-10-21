import os
from configuration.configuration import CSV_DIR

def save_dataframe_to_csv(df, date_column, csv_dir=CSV_DIR, row_index=1):

    # Retrieve the date from the DataFrame (adjust indexing as needed)
    actual_date = df.loc[row_index, date_column]

    # Ensure the date is in the correct format (if it's not already a string)
    if not isinstance(actual_date, str):
        actual_date_str = actual_date.strftime('%Y-%m-%d')
    else:
        actual_date_str = actual_date

    # # Remove any invalid characters for filenames
    # actual_date_str = actual_date_str.replace(':', '-').replace('/', '-').replace(' ', '_')

    # Define the directory and filename with the date
    filename = f'CSV_output_{actual_date_str}.csv'
    filepath = os.path.join(csv_dir, filename)

    # Create the directory if it doesn't exist
    os.makedirs(csv_dir, exist_ok=True)

    # Save the DataFrame to the CSV file
    df.to_csv(filepath, index=False)
