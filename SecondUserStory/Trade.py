from datetime import datetime
from binance import Client
import pandas as pd
from google.cloud import bigquery
import json
from google.oauth2 import service_account
from google.auth import jwt

binance_api_key = 'VvriEf3Lvl74QqdG8XnxeaUV2YyJ2u56ecRg8gh1Mb7mx7BAWUlrDC87nHEBIi40'
binance_api_secret = 'LwfOr7t6E9eqqG9JtxNacWTkvoKkI9Ftmr7JbYQ29MOWCL33x6nSdCbYbv04Ya8A'
bi_client = Client(binance_api_key, binance_api_secret)
listOfCoins={"XRPUSDT","BTCUSDT","ETHUSDT","ADAUSDT","LTCUSDT","HBARUSDT","BNBUSDT","SOLUSDT",
             "DOTUSDT","DOGEUSDT","UNIUSDT","XLMUSDT","MATICUSDT","AVAXUSDT","LINKUSDT","ALGOUSDT",
             "TRXUSDT"}
hist_df = pd.DataFrame(columns= ['Open_Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close_Time', 'Quote_Asset_Volume', 
                    'Number_of_Trades', 'TB_Base_Volume', 'TB_Quote_Volume', 'Ignore','Trader'])
for i in listOfCoins:
#client.get_historical_klines??
    historical = bi_client.get_historical_klines(i, Client.KLINE_INTERVAL_1DAY, '1 Jan 2011')
#print(historical)
    hist_df_1 = pd.DataFrame(historical)
    hist_df_1.columns = ['Open_Time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close_Time', 'Quote_Asset_Volume', 
                    'Number_of_Trades', 'TB_Base_Volume', 'TB_Quote_Volume', 'Ignore']
#print(hist_df.head())
    hist_df_1['Trader'] = i
    hist_df = pd.concat([hist_df,hist_df_1], ignore_index=True)
#preprocess
hist_df['Open_Time'] = pd.to_datetime(hist_df['Open_Time']/1000, unit='s')
hist_df['Close_Time'] = pd.to_datetime(hist_df['Close_Time']/1000, unit='s')
numeric_columns = ['Open', 'High', 'Low', 'Close', 'Volume', 'Quote_Asset_Volume', 'TB_Base_Volume', 'TB_Quote_Volume']
hist_df[numeric_columns] = hist_df[numeric_columns].apply(pd.to_numeric, axis=1)
hist_df=hist_df.astype(str)
print(hist_df.head())
project_id="data-management-project-379718"
dataset_id = "test_2"
table="data22222"
table_id = "data-management-project-379718.test_2.data22222"
key_path= "/Users/tanyagoyal/Downloads/data-management-project-379718-c1596129d77a.json"
credentials = service_account.Credentials.from_service_account_file(key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])
bq_client = bigquery.Client(credentials=credentials, project=project_id)
dataframe=hist_df
job_config = bigquery.LoadJobConfig(
    # Specify a (partial) schema. All columns are always written to the
    # table. The schema is used to assist in data type definitions.
    schema=[
        bigquery.SchemaField("Open_Time", bigquery.enums.SqlTypeNames.STRING),
        # Specify the correct data types for other columns in the schema
        bigquery.SchemaField("Open", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("High", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Low", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Close", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Volume", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Close_Time", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Quote_Asset_Volume", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Number_of_Trades", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("TB_Base_Volume", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("TB_Quote_Volume", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Ignore", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Trader", bigquery.enums.SqlTypeNames.STRING),
  
    ],

    write_disposition="WRITE_TRUNCATE",
)
job = bq_client.load_table_from_dataframe(
    dataframe, table_id
)  # Make an API request.
job.result()  # Wait for the job to complete.

table = bq_client.get_table(table_id)  # Make an API request.
print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), table_id
    )
)