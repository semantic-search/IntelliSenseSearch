from db_models.mongo_setup import global_init
from db_models.models.cache_model import Cache
from task_worker.celery import celery_app
from bert_serving.client import BertClient
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import globals
import json

global_init()
bc = BertClient(output_fmt='list')
client = Elasticsearch('localhost:9200')
#
#
# def location_index(location):
#     image_location={}
#     location_address={}
#     image_location=location
#     location_address=image_location["location_address"]
#     string_index=''
#
#     if location_address["city"]:
#
#         string_index=image_location["location_category"]+location_address['location_name']+\
#                      location_address['city']+location_address['county']\
#                  +location_address['state_district']+location_address['state']+location_address['country']
#
#     elif location_address["village"]:
#         string_index = image_location["location_category"] + location_address['location_name'] +\
#                        location_address['village']+ location_address['county'] \
#                        + location_address['state_district'] + location_address['state'] + location_address['country']
#     print(type(string_index))
#     return string_index
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
        # '_op_type': 'index',
        # '_index': "semantic",
        'text': doc['text'],
        'file_name': doc['file_name'],
        # 'image_location': location_string,
        'doc_id': doc['doc_id'],
        'text_vector': emb,

    }



def bulk_predict(docs, batch_size=256):
    """Predict bert embeddings."""
    for i in range(0, len(docs), batch_size):
        batch_docs = docs[i: i+batch_size]
        embeddings = bc.encode([doc['text'] for doc in batch_docs])
        for emb in embeddings:
            yield emb
# @celery_app.task()
def process_index_doc(id):
    bc = BertClient(output_fmt='list')
    client = Elasticsearch('localhost:9200')
    # client = Elasticsearch('localhost:9200')
    print(f"id {type(id)}")
    db_obj = Cache.objects.get(id=id)
    document = {}
    document["doc_id"] = id
    print("THIS IS FUCKING ID BITCH",id)
    document["file_name"] = getVal(db_obj, "file_name")
    document["text"] = getVal(db_obj, "text")
    print("ABOVE TRY OF LOCATION")
    # print("****************"+document["text"]+"****************")

    if len(document["text"])==0:
        document["emb"]=[0]
    else:
        print("ATLEAST CAME IN ELSE")
        # document["emb"]= bc.encode([document["text"]])
        # document["emb"]= bc.encode(["India is a amazing country where once God lived"])

        # print(document["emb"])
    to_bulk=[]

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


    # print(f"file {db_obj.file_name} indexed in elasticsearch")

# res = es.index(index='company',doc_type='employee',id=1,body=e1, request_timeout=45)