import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
from google.cloud import bigquery
import concurrent.futures
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import train_test_split, GridSearchCV
from google.cloud import storage
import pickle
from google.cloud import secretmanager
from google.oauth2 import service_account
import json

secret_client = secretmanager.SecretManagerServiceClient()
project_id = 'data-engineering-2-2023'  
dataset_id = 'dataframes'
secret_id ="ShahriBabaki"
secret_version =1
secret_name = f"projects/{project_id}/secrets/{secret_id}/versions/{secret_version}"

# Access the secret value
response = secret_client.access_secret_version(name=secret_name)
data = json.loads(response.payload.data.decode("utf-8"))
credentials = service_account.Credentials.from_service_account_info(data)
client = bigquery.Client(credentials=credentials)
dataset_ref = client.dataset(dataset_id, project=project_id)

# List all tables in the dataset
tables = client.list_tables(dataset_ref)


def fetch_table_data(table):
    table_ref = dataset_ref.table(table.table_id)
    table_data = client.get_table(table_ref)
    dataframe = client.list_rows(table_data).to_dataframe()
    return table.table_id, dataframe

dataframes = {}  # Dictionary to store DataFrames

with concurrent.futures.ThreadPoolExecutor() as executor:
    # Submit tasks to the executor
    future_to_table = {executor.submit(fetch_table_data, table): table for table in tables}

    # Retrieve results as they become available
    for future in concurrent.futures.as_completed(future_to_table):
        table = future_to_table[future]
        try:
            table_id, dataframe = future.result()
            dataframes[table_id] = dataframe
        except Exception as e:
            print(f"Error fetching table {table.table_id}: {str(e)}")
client = storage.Client(project=project_id)

# Define the project ID and bucket name
bucket_name = 'models-ml-2023'

try:
    bucket = client.create_bucket(bucket_name)
except:
    bucket = client.get_bucket(bucket_name)


for table_id, dataframe in dataframes.items():
    #print(f"Table ID: {table_id}")
    #print(dataframe.head())  # Example: Print the first few rows of the DataFrame
    data = dataframe.dropna(subset=["DECISION"])
    X = data[["DirectionMovingLong","DirectionMovingShort","rsiLong30","rsiShort70","macdShort","macdLong","MfiStochShort","MfiStochLong","StochShort","StochLong"]].values
    y = data["DECISION"].values
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=4)

    param_grid = {'n_neighbors': [1, 3, 4, 5, 7, 9]}

    # Initialize the K-nearest neighbors classifier
    knn = KNeighborsClassifier()

    # Perform grid search with cross-validation
    grid_search = GridSearchCV(knn, param_grid, cv=4)
    grid_search.fit(X_train, y_train)

    # Get the best parameter values and the corresponding model
    best_params = grid_search.best_params_
    best_model = grid_search.best_estimator_
    model_bytes = pickle.dumps(best_model)

    # Upload the model bytes to the GCS bucket
    blob = client.bucket(bucket_name).blob(f'{table_id}.pkl')
    blob.upload_from_string(model_bytes, content_type='application/pickle')


data = dataframes["XRPUSDT1m"].dropna(subset=["DECISION"])
print(data)

X = data[["DirectionMovingLong","DirectionMovingShort","rsiLong30","rsiShort70","macdShort","macdLong","MfiStochShort","MfiStochLong","StochShort","StochLong"]].values     

y=data["DECISION"].values
X_train, X_test, y_train, y_test = train_test_split( X, y, test_size=0.2, random_state=4)

param_grid = {'n_neighbors': [1, 3,4, 5, 7, 9]}

# Initialize the K-nearest neighbors classifier
knn = KNeighborsClassifier()

# Perform grid search with cross-validation
grid_search = GridSearchCV(knn, param_grid, cv=5)
grid_search.fit(X_train, y_train)

# Get the best parameter values and the corresponding model
best_params = grid_search.best_params_
best_model = grid_search.best_estimator_
print(best_params)
# Make predictions on the test data using the best model
predictions = best_model.predict(X_test)

# Evaluate the best model's accuracy
accuracy = best_model.score(X_test, y_test)






