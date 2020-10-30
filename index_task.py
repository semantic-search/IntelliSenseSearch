from db_models.mongo_setup import global_init
from db_models.models.cache_model import Cache
from dotenv import load_dotenv
load_dotenv()
from bert_serving.client import BertClient
from elasticsearch import Elasticsearch
import globals
import json
import os

global_init()
bc = BertClient(output_fmt='list')
client = Elasticsearch(os.getenv("ELASTIC_SEARCH_HOST"))

def getVal(db_obj, key: str, error_res=""):
    try:
        print(type(key), "key")
        val = db_obj[key]
        if val is None:
            return error_res
        return val
    except KeyError:
        return error_res

def create_document(doc,emb):
    print("IN CREATE DOCUMENT")
    return {

        'text': doc['text'],
        'file_name': doc['file_name'],
        'doc_id': doc['doc_id'],
        'text_vector': emb,
        'url': ''

    }


def bulk_predict(docs, batch_size=256):
    ''' Predict bert embeddings. '''
    for i in range(0, len(docs), batch_size):
        batch_docs = docs[i: i+batch_size]
        embeddings = bc.encode([doc['text'] for doc in batch_docs])
        for emb in embeddings:
            yield emb

def process_index_doc(id):
    bc = BertClient(output_fmt='list')
    client = Elasticsearch('localhost:9200')

    print(f"id {type(id)}")
    
    db_obj = Cache.objects.get(id=id)
    document = {}
    document["doc_id"] = id
    print("THIS IS THE ID ",id)
    document["file_name"] = getVal(db_obj, "file_name")
    document["text"] = getVal(db_obj, "text")


    # document["emb"]= bc.encode([document["text"]])

    # to_bulk=[]

    to_index=[document]
    for doc, emb in zip(to_index, bulk_predict(to_index)):
        d= create_document(doc, emb)
        # to_bulk.append(d)
        print("SEND TO INDEX",d)
        res = client.index(index='semantic', body=d)
        print(res)
    print("DONE INDEXING SUCESS")

    # print("THIS SHIT HAS TO BE INDEXED",to_bulk)
    # to_index.append(to_bulk)
    # bulk(client, to_bulk)
    # client.bulk(to_bulk)
    # bulk(client, to_index)



