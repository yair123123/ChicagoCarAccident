import pytest
from pymongo import MongoClient
from config.connect import *
from repository.csv_repository import init_chicago_accidents


@pytest.fixture(scope="function")
def mongodb_client():
   client = MongoClient('mongodb://172.29.168.75:27017')
   yield client
   client.close()


@pytest.fixture(scope="function")
def taxi_db_test(mongodb_client):
   db_name = 'test_chicago_car_accident'
   db = mongodb_client[db_name]
   yield db
   mongodb_client.drop_database(db_name)


@pytest.fixture(scope="function")
def init_test_data(taxi_db_test):
   # Initialize the main database if it's empty
   if taxi_db['accidents'].count_documents({}) == 0:
       init_chicago_accidents()


   # Copy data from main database to test database
   for collection_name in taxi_db.list_collection_names():
       taxi_db_test[collection_name].drop()
       taxi_db_test[collection_name].insert_many(taxi_db[collection_name].find())


   yield taxi_db_test


   # Clean up test data after each test
   for collection_name in taxi_db_test.list_collection_names():
       taxi_db_test[collection_name].drop()
