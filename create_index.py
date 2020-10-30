"""
Example script to create elasticsearch index.
"""


from elasticsearch import Elasticsearch


def index():
    client = Elasticsearch('13.68.241.106:9200')
    client.indices.delete(index="semantic", ignore=[404])
    with open("index.json") as index_file:
        source = index_file.read().strip()
        client.indices.create(index="semantic", body=source)

index()

    # python3 example/create_index.py --index_file=index.json --index_name=jobsearch
