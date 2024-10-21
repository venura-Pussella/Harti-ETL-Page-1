import pandas as pd

def create_dataframe(dates, categories, items, pettah_price_ranges, pettah_averages):
    df = pd.DataFrame({
        'Date': dates,
        'Category': categories,
        'Item': items,
        'Pettah Price Range': pettah_price_ranges,
        'Pettah Average': pettah_averages
    })
    
    return df
