import pandas as pd
import os
from env import get_connection


def tsa_import():
    '''
    imports all features from sales, items, and stores tables from 
    `tsa_item_demand` from the codeup SQl server
    minus item and store ids
    '''
    filename = 'tsa_items'
    
    if os.path.isfile(filename):
        return pd.read_csv(filename)
    
    else:
        query = '''
        SELECT  sale_date, sale_amount, item_brand, item_name, item_price, 
        store_address, store_zipcode, store_city, store_state
        FROM sales
        LEFT JOIN items USING(item_id)
        LEFT JOIN stores USING(store_id)
        ;
        '''
        url = get_connection('tsa_item_demand')
        df = pd.read_sql(query,url)
        df.to_csv(filename, index=False)
        
        return df
    
    
def convert_datetime(df):
    '''
    converts date colum to date time object and sets it as index. 
    sorts df by date time object
    '''
    df.sale_date = pd.to_datetime(df.sale_date)
    df = df.set_index('sale_date')
    df = df.sort_values('sale_date')
    
    return df


def prep_tsa(df):
    '''
    creates columns in the tsa df.
    '''
    df['month']= df.index.month_name()
    df['day_of_week'] = df.index.day_name()
    df['sales_total']=df.sale_amount * df.item_price
    
    return df


def tsa_item_demand_pipeline():
    '''
    runs the tsa pipeline
    '''
    df = tsa_import()
    df = convert_datetime(df)
    df = prep_tsa(df)
    
    return df