from db_models.mongo_setup import global_init
from db_models.models.web_model import Web

from task_worker.celery import celery_app
from bert_serving.client import BertClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import globals
import json

global_init()
bc = BertClient(output_fmt='list')
client = Elasticsearch('13.68.241.106:9200')

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
        'doc_id': doc['doc_id'],
        'url': doc['url'],
        'file_name': '',
        'text_vector': emb,

    }


def bulk_predict(docs, batch_size=256):
    ''' Predict bert embeddings. '''
    for i in range(0, len(docs), batch_size):
        batch_docs = docs[i: i+batch_size]
        embeddings = bc.encode([doc['text'] for doc in batch_docs])
        for emb in embeddings:
            yield emb


def process_url_doc(id):
    bc = BertClient(output_fmt='list')
    client = Elasticsearch('localhost:9200')

    print(f"id {type(id)}")

    db_obj = Web.objects.get(id=id)
    document = {}
    document["doc_id"] = id
    document["text"] = getVal(db_obj, "text")
    print("THIS IS THE ID ", id)
    document["url"]=getVal(db_obj, "url")
    to_index = [document]
    for doc, emb in zip(to_index, bulk_predict(to_index)):
        d = create_document(doc, emb)
        # to_bulk.append(d)
        print("SEND TO INDEX", d)
        res = client.index(index='semantic', body=d)
        print(res)
    print("DONE INDEXING SUCCESS")

