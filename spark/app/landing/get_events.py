import os
import requests
import hashlib
from dotenv import load_dotenv
# from pyspark.sql import SparkSession
import argparse

# load_dotenv()
# MARVEL_PUBLIC_KEY = os.getenv('MARVEL_PUBLIC_KEY')
# MARVEL_PRIVATE_KEY = os.getenv('MARVEL_PRIVATE_KEY')

# spark = SparkSession.builder.appName('get-events').getOrCreate()
# sc = spark.sparkContext
parser = argparse.ArgumentParser()
parser.add_argument('--marvel_public_key', type=str)
parser.add_argument('--marvel_private_key', type=str)
parser.add_argument('--ts', type=str)
args = parser.parse_args()

ts = args.ts

ts = 1678428168  # int(time.time())
output_folder = f"./case/landing/events/uploaded_at={ts}"
print(f"output_folder: {output_folder}")
os.makedirs(os.path.dirname(f"{output_folder}/"), exist_ok=True)

# Event Characters
event_char_output_folder = f"./case/landing/event_characters/uploaded_at={ts}"
print(f"event_char_output_folder: {event_char_output_folder}")
os.makedirs(os.path.dirname(f"{event_char_output_folder}/"), exist_ok=True)


def make_url_params(ts):
    # public_key = MARVEL_PUBLIC_KEY
    # private_key = MARVEL_PRIVATE_KEY
    public_key = args.marvel_public_key
    private_key = args.marvel_private_key
    hash_input = f"{ts}{private_key}{public_key}"
    hsh = hashlib.md5(hash_input.encode('utf-8')).hexdigest()
    param_str = f"?ts={ts}&apikey={public_key}&hash={hsh}"
    return param_str


def get_batch_number(url):
    url_with_limit = f"{url}&limit=1"
    response = requests.request("GET", url_with_limit)
    response_json = response.json()
    total = response_json['data']['total']
    return total // 100


def get_event_character_list(result, url_param_str):
    event_id = result['id']
    characters = result['characters']
    available = characters['available']
    print(f"Event: {event_id}, Available: {available}")
    if available > 20:
        collection_uri = characters['collectionURI']
        collection_uri_with_perms = f"{collection_uri}{url_param_str}"
        ec_batches = available // 100
        for batch in range(ec_batches + 1):
            offset = batch * 100
            url_with_limit_and_offset = f"{collection_uri_with_perms}&limit=100&offset={offset}"
            response = requests.request("GET", url_with_limit_and_offset)
            ec_output_file = f"{event_char_output_folder}/char_events_{event_id}_{batch}.json"
            with open(ec_output_file, "w+") as ec_file:
                ec_file.write(response.text)
                print(f"Response: {response.status_code} Written EC: {ec_output_file}")
    return


def get_batch(url, url_params, idx):
    offset = idx * 100
    url_with_limit_and_offset = f"{url}&limit=100&offset={offset}"
    response = requests.request("GET", url_with_limit_and_offset)
    response_txt = response.text
    output_file = f"{output_folder}/events_{idx}.json"
    with open(output_file, "w+") as file:
        file.write(response_txt)
        print(f"Written: {idx}")
    response_json = response.json()
    results = response_json['data']['results']
    for result in results:
        get_event_character_list(result, url_params)
    return output_file


url_params = make_url_params(ts)
base_url = f"http://gateway.marvel.com/v1/public/events{url_params}"
batches = get_batch_number(base_url)
# rdd = sc.parallelize(range(batches + 1))
# rdd.map(lambda i: get_batch(base_url, url_params, i)).collect()

for batch in range(batches + 1):
    get_batch(base_url, url_params, batch)
