import requests
import os
import time
import hashlib
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()
MARVEL_PUBLIC_KEY = os.getenv('MARVEL_PUBLIC_KEY')
MARVEL_PRIVATE_KEY = os.getenv('MARVEL_PRIVATE_KEY')

spark = SparkSession.builder.appName('get-characters').getOrCreate()
sc = spark.sparkContext

ts = int(time.time())
output_folder = f"./case/landing/characters/uploaded_at={ts}"
print(f"output_folder: {output_folder}")
os.makedirs(os.path.dirname(output_folder), exist_ok=True)


def make_url_params(ts):
    public_key = MARVEL_PUBLIC_KEY
    private_key = MARVEL_PRIVATE_KEY
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


def get_batch(url, idx):
    offset = idx * 100
    url_with_limit_and_offset = f"{url}&limit=100&offset={offset}"
    response = requests.request("GET", url_with_limit_and_offset)
    response_txt = response.text
    output_file = f"{output_folder}/characters_{idx}.json"
    with open(output_file, "w+") as file:
        file.write(response_txt)
        print(f"Written: {idx}")
    return output_file


url_params = make_url_params(ts)
base_url = f"http://gateway.marvel.com/v1/public/characters{url_params}"
batches = get_batch_number(base_url)
rdd = sc.parallelize(range(batches + 1))
rdd.map(lambda i: get_batch(base_url, i)).collect()
