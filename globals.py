import os
from dotenv import load_dotenv
load_dotenv()
MONGO_HOST = os.getenv("MONGO_HOST")
DB = os.getenv('MONGO_DB')
PORT = os.getenv('MONGO_PORT')
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
ELASTIC_SEARCH_HOST=os.getenv("ELASTIC_SEARCH_HOST")
REDIS_HOSTNAME = os.getenv("REDIS_HOSTNAME")
REDIS_PORT = os.getenv("REDIS_PORT")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")
REDIS_DB = '0'
