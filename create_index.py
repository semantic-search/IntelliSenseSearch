"""
Example script to create elasticsearch index.
"""


from elasticsearch import Elasticsearch
import os
from dotenv import load_dotenv

load_dotenv()

def index():
    client = Elasticsearch(os.getenv("ELASTIC_SEARCH_HOST"))
    client.indices.delete(index="semantic", ignore=[404])
    with open("index.json") as index_file:
        source = index_file.read().strip()
        client.indices.create(index="semantic", body=source)

index()

    # python3 example/create_index.py --index_file=index.json --index_name=jobsearch
