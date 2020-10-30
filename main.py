from db_models.mongo_setup import global_init
from db_models.models.cache_model import Cache
from db_models.models.web_model import Web
from index_urls import process_url_doc
import globals
import os
import time
from index_task import process_index_doc
from elasticsearch import Elasticsearch
from run_queue_worker import q
import os

client = Elasticsearch(globals.ELASTIC_SEARCH_HOST)

global_init()
# if not client.indices.exists(index="semantic"):
#     print("creating_index")
#     os.system("python3 create_index.py")
a=time.time()
for file in Cache.objects:
    id = file.id
    if file.text:
        print(type(str(id)))
        #process_index_doc((str(id)))
        q.enqueue(process_index_doc, (str(id)))


for site in Web.objects:
    id = site.id
    if site.text:
        print(type(str(id)))
        #process_url_doc((str(id)))
        q.enqueue(process_url_doc, (str(id)))
b=time.time()
print(b-a)
