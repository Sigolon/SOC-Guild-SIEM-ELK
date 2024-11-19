import requests
from requests.auth import HTTPBasicAuth
import os

def bulk_api_upload(target_url, index, bulk_data):
    headers = {
        "Content-Type": "application/json"
    }

    password = "" # modify this
    ca_crt = "" # modify this 
    auth = HTTPBasicAuth('elastic', f'{password}')

    response = requests.post(url=target_url, headers=headers, data=bulk_data, verify=ca_crt, auth=auth, timeout=10)
    return response.status_code

def upload_error_data_store(store_error_ndjson_path, ndjson_data):
    if not os.path.exists(store_error_ndjson_path):
        with open(store_error_ndjson_path, "w") as f:
            pass

    if not isinstance(ndjson_data, str):
        raise ValueError("ndjson_data should be a string")

    with open(store_error_ndjson_path, "a") as error_file:
        if not ndjson_data.endswith("\n"):
            ndjson_data += "\n"
        error_file.write(ndjson_data)

ndjson_file = "detection-hackathon-apt29/datasets/day1/apt29_evals_day1_manual_2020-05-01225525.json"
target_url = "https://localhost:9200/_bulk?pretty"
index = "" # modify this
store_error_ndjson_path = "detection-hackathon-apt29/datasets/day1/error_upload_data.ndjson"

with open(ndjson_file, "r") as f:
    ndjson_data_lines = f.readlines()

bulk_data = ""
counter = 0


BULK_SIZE = 500
for ndjson_data in ndjson_data_lines:
    bulk_data += f'{{ "index" : {{ "_index" : "{index}" }} }}\n'
    bulk_data += ndjson_data
    counter += 1

    if counter % BULK_SIZE == 0:
        response_status = bulk_api_upload(target_url, index, bulk_data)
        if response_status != 200:
            upload_error_data_store(store_error_ndjson_path, bulk_data)
        bulk_data = "" 

if bulk_data:
    response_status = bulk_api_upload(target_url, index, bulk_data)
    if response_status != 200:
        upload_error_data_store(store_error_ndjson_path, bulk_data)



