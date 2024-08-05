from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient


load_dotenv(find_dotenv())
DB_USER = os.environ.get("DB_USER_TEST")
DB_PWD = os.environ.get("DB_PWD_TEST")
DB_CODE = os.environ.get("DB_CODE_TEST")

connection_string = f'mongodb+srv://{DB_USER}:{DB_PWD}@tutorial.{DB_CODE}.mongodb.net/?retryWrites=true&w=majority&appName=tutorial'

client = MongoClient(connection_string)

dbs = client.list_database_names()
# print(dbs)
test_db = client.testDB
collection = test_db.list_collection_names()
# print(collection)

# --- INSERT DOC ---
# insert Document 
def insert_test_doc():
    collection = test_db.testDB
    test_document = {
        "name": "Ben",
        "type": "test"
    }
    inserted_id = collection.insert_one(test_document).inserted_id
    print(inserted_id) # print Document BSON ID
    
# insert_test_doc()


# --- INSERT MULTIPLE DOCS ---

production = client.production # it will automatically create a database if production database does not exist
person_collection = production.person_collection 

def create_documents():
    first_names = ["Ben", 'Sarah', "Jennifer", 'Jose', "Brad", 'Allen']
    last_names = ['Zojaji', "Bart", 'Pit', 'Smith', "Cater", 'Geral']
    ages = [24, 40, 23, 19, 34, 67]
    
    docs = []
    for first_name , last_name, age in zip(first_names, last_names, ages):
        doc = {"first_name": first_name, "last_name": last_name, "age": age}
        docs.append(doc)
        # person_collection.insert_one(doc)
        
    person_collection.insert_many(docs)
    
# create_documents()





# --- READING DOCUMENTS ---
printer = pprint.PrettyPrinter()
def find_all_people():
    people = person_collection.find()
    
    for person in people:
        printer.pprint(person)

# find_all_people()


def find_name(name='Ben'):
    name = person_collection.find_one({'first_name':f'{name}'}) # {'first_name':f'{name}', 'age': 24} or use .find to return all the docs that match the condition
    
    printer.pprint(name)

# find_name()


def count_all_people():
    count = person_collection.count_documents(filter={})  # or person_collection.find().count()
    print('Number of people ', count)
    
# count_all_people()


def get_person_by_id(person_id):
    from bson.objectid import ObjectId
    
    _id = ObjectId(person_id)
    person = person_collection.find_one({'_id': _id})
    printer.pprint(person)

# get_person_by_id('66aba4c37e986ca0c53e552a')


def get_age_range(min_age, max_range):
    query = {"$and": [
            {'age': {'$gte': min_age}},
            {'age': {'$lte': max_range}}
        ]}
    people = person_collection.find(query).sort('age')
    for person in people:
        printer.pprint(person)
        
# get_age_range(20, 35)
    

def project_column():
    columns = {'_id': 0, 'first_name': 1, 'last_name': 1}
    people = person_collection.find({}, columns)
    for person in people:
        printer.pprint(person)
        
# project_column()



# --- UPDATING DOCUMENTS ---
def update_person_by_id(person_id):
    from bson.objectid import ObjectId
    
    _id = ObjectId(person_id)
    
    # all_updates = {
    #     "$set": {'new_field': True}, 
    #     "$inc": {'age': 1},
    #     "$rename": {'first_name': 'first', 'last_name': 'last'}
    # }
    # person_collection.update_one({'_id': _id}, all_updates)
    
    person_collection.update_one({'_id': _id}, {'$unset': {'new_field': ""}})
    
# update_person_by_id('66aba4c37e986ca0c53e552a')



def replace_one(person_id): # keep id but change the whole doc
    from bson.objectid import ObjectId
    
    _id = ObjectId(person_id)
    
    new_doc = {
        'first_name': 'new first name',
        'last_name': 'new last name',
        'age': 100
    }
    
    person_collection.replace_one({'_id': _id}, new_doc)
    
# replace_one('66aba4c37e986ca0c53e552a')



# --- DELETE DOCUMENT ---
def delete_doc_by_id(person_id):
    from bson.objectid import ObjectId
    
    _id = ObjectId(person_id)
    person_collection.delete_one({"_id":_id})
    
# delete_doc_by_id('66aba4c37e986ca0c53e552a')



# --- RELATIONSHIP ---
"""
# option 1: Document Embedding -> copy and paste it (works when you have 1 section) - good for 1 to 1 

address = {
    "_id": '62475964011a9126a4cebeb7',
    "street": "bay street",
    'number': 2706,
    'city': 'San francisco',
    'country': 'United States',
    'zip': '94107'
}
person = {
    '_id': '62475964011a9126a4',
    'first_name': 'John',
    'address': {
        "_id": '62475964011a9126a4cebeb7',
        "street": "bay street",
        'number': 2706,
        'city': 'San francisco',
        'country': 'United States',
        'zip': '94107'
        }
}


# option 2: good for 1 to many   or   many to many
address = {
    "_id": '62475964011a9126a4cebeb7',
    "street": "bay street",
    'number': 2706,
    'city': 'San francisco',
    'country': 'United States',
    'zip': '94107',
    'owner_id': '62475964011a9126a4'
}
person = {
    '_id': '62475964011a9126a4',
    'first_name': 'John'
}
"""


address = {
    "_id": '62475964011a9126a4cebeb7',
    "street": "bay street",
    'number': 2706,
    'city': 'San francisco',
    'country': 'United States',
    'zip': '94107'
}


# option 1
def add_address_embed(person_id, address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    
    person_collection.update_one({"_id": _id}, {"$addToSet": {'addresses': address}})
    
# add_address_embed('66aba4c37e986ca0c53e5529', address)


# option 2
def add_address_relationship(person_id, address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    
    address = address.copy()
    address['owner_id'] = person_id
    
    address_collection = production.address
    address_collection.insert_one(address)
    
# add_address_relationship('66aba4c37e986ca0c53e552b', address)
