from google.cloud import bigquery
from google.cloud import storage
from google.cloud import secretmanager
from google.oauth2 import service_account
import concurrent.futures
import json
import pickle
import pandas as pd
project_id = 'data-engineering-2-2023' 
dataset_id = 'dataframes' 
bucket_name ='models-ml-2023'
secret_name ="ShahriBabaki"
secret_id ="ShahriBabaki"
secret_version =1
secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/{secret_version}"
secret_client = secretmanager.SecretManagerServiceClient() 
# Access the secret value
response = secret_client.access_secret_version(name=secret_name)
data = json.loads(response.payload.data.decode("utf-8"))
credentials = service_account.Credentials.from_service_account_info(data)
bq_client = bigquery.Client(credentials=credentials)

storage_client = storage.Client(credentials=credentials)

dataset_ref = bq_client.dataset(dataset_id, project=project_id)

bucket = storage_client.get_bucket(bucket_name)

def fetch_table_data(table_id):
    table_ref = dataset_ref.table(table_id)
    df = bq_client.get_table(table_ref)
    df = bq_client.list_rows(df).to_dataframe()  
    df=df.iloc[[-1]]
    date=df["date"]
    X = df[["DirectionMovingLong","DirectionMovingShort","rsiLong30","rsiShort70","macdShort","macdLong","MfiStochShort","MfiStochLong","StochShort","StochLong"]].values
    model_file_path = f'{table_id}.pkl'  
    blob = bucket.blob(model_file_path)
    model_bytes = blob.download_as_bytes()  # Download the model file as bytes
    model = pickle.loads(model_bytes)
    predictions = model.predict(X)

    return table_id, predictions,date
results = {}
with concurrent.futures.ThreadPoolExecutor() as executor:
    futures = [executor.submit(fetch_table_data, table.table_id) for table in bq_client.list_tables(dataset_ref)]
    for future in concurrent.futures.as_completed(futures):
        try:
            table_id, predictions, date = future.result()
            results[table_id] = predictions
            results["date"]=date
        except Exception as e:
            print(f"Error fetching table data or performing predictions: {str(e)}")

df = pd.DataFrame.from_dict(results)
shifted_column = df.pop("date")

# Insert the shifted column at the first position
df.insert(0,"date", shifted_column)
print(df)



#df=DataFrame2.iloc[[-1]]
#X = data[["DirectionMovingLong","DirectionMovingShort","rsiLong30","rsiShort70","macdShort","macdLong","MfiStochShort","MfiStochLong","StochShort","StochLong"]].values
#y_predict=neigh.predict(X)
#if y_predict in ["SELL1", "SELL2", "BUY1", "BUY2"]:
    #if y_predict in ["SELL1", "SELL2"]:
        #"Short"
    #else:
        #"Long"
    #print("Condition satisfied")
#else:
#
#
    #print("Condition not satisfied")
