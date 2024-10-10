from pymongo import MongoClient


client = MongoClient('mongodb://172.29.168.75:27017')
taxi_db = client['chicago_car_accident']


monthly = taxi_db['monthly']
weekly = taxi_db['weekly']
daily = taxi_db['daily']
accidents = taxi_db['accident']
areas = taxi_db["areas"]