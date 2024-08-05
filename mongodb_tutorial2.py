from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient
from datetime import datetime as dt


load_dotenv(find_dotenv())
DB_USER = os.environ.get("DB_USER_TEST")
DB_PWD = os.environ.get("DB_PWD_TEST")
DB_CODE = os.environ.get("DB_CODE_TEST")

connection_string = f'mongodb+srv://{DB_USER}:{DB_PWD}@tutorial.{DB_CODE}.mongodb.net/?retryWrites=true&w=majority&appName=tutorial&authSource=admin'

client = MongoClient(connection_string)

dbs = client.list_database_names()
library = client.library

# --- Schema Validation ---

def create_book_collection():
    book_validator = {
        '$jsonSchema': {
            'bsonType': "object",
            'required': [ "title", "authors", "publish_date", "type", 'copies'],
            'properties': {
                'title': {
                'bsonType': "string",
                'description': "must be a string and is required"
                },
                'authors': {
                'bsonType': "array",
                'items':{
                    'bsonType': 'objectId',
                    'description': "must be an objectid and is required"
                }
                },
                'publish_date': {
                'bsonType': "date",
                'description': "must be a date and is required"
                },
                'type': {
                'enum': ['Fiction', 'Non-Fiction'],
                'description': "can only be one of the enum values and is required"
                },
                'copies': {
                'bsonType': "int",
                'minimum': 0,
                'description': "must be an integer greater then 0 and is required"
                }
            }
        }
    }

    try:
        library.create_collection('book')
    except Exception as e:
        print(e)
        
    library.command("collMod", 'book', validator=book_validator)

# create_book_collection()


def create_author_collection():
    author_validator = {
        '$jsonSchema': {
            'bsonType': "object",
            'required': [ "first_name", "last_name", "date_of_birth"],
            'properties': {
                'first_name': {
                'bsonType': "string",
                'description': "must be a string and is required"
                },
                'last_name': {
                'bsonType': "string",
                'description': "must be a string and is required"
                },
                'date': {
                'bsonType': "date",
                'description': "must be a date and is required"
                }
            }
        }
    }
    
    try:
        library.create_collection('author')
    except Exception as e:
        print(e)
        
    library.command("collMod", 'author', validator=author_validator)

# create_author_collection()



# --- Bulk Insert ---

def create_data():
    authors = [
        {
        'first_name': 'Ben',
        'last_name': 'Zojaji',
        'date_of_birth': dt(2000, 1, 1)
        },
        {
        'first_name': 'George',
        'last_name': 'Orwell',
        'date_of_birth': dt(1903, 6, 25)
        },
        {
        'first_name': 'Herman',
        'last_name': 'Melville',
        'date_of_birth': dt(1819, 8, 1)
        },
        {
        'first_name': 'F. Scott',
        'last_name': 'Fitzgerald',
        'date_of_birth': dt(1896, 9, 24)
        },
    ]
    author_collection = library.author
    authors = author_collection.insert_many(authors).inserted_ids
    
    books = [
        {
            'title': 'MongoDB Advance Tutorial',
            'authors': [authors[0]],
            'publish_date': dt.today(),
            'type': "Non-Fiction",
            'copies': 5
        },
        {
            'title': 'Python for Dummies',
            'authors': [authors[0]],
            'publish_date': dt(2022, 1, 17),
            'type': "Non-Fiction",
            'copies': 5
        },
        {
            'title': 'Nineteen Eighty-four',
            'authors': [authors[1]],
            'publish_date': dt(1949, 6, 8),
            'type': "Fiction",
            'copies': 5
        },
        {
            'title': 'The Great Gatsby',
            'authors': [authors[3]],
            'publish_date': dt(2014, 5, 23),
            'type': "Fiction",
            'copies': 5
        },
        {
            'title': 'Moby Dick',
            'authors': [authors[2]],
            'publish_date': dt(1851, 9, 24),
            'type': "Fiction",
            'copies': 5
        }
    ]
    book_collection = library.book
    book_collection.insert_many(books)
    
# create_data()


printer = pprint.PrettyPrinter()
# --- Advanced Queries ---
# ex. 1
# books_containing_a = library.book.find({'title': {'$regex': 'a{1}'}})
# printer.pprint(list(books_containing_a))




# ex. 2
# authors_and_books = library.author.aggregate([{
#     '$lookup': {
#         'from': 'book',
#         'localField': '_id',
#         'foreignField': 'authors',
#         'as': 'books'
#     }
# }])
# printer.pprint(list(authors_and_books))



# ex. 3
# authors_book_count = library.author.aggregate([
#     {
#         '$lookup': {
#             'from': 'book',
#             'localField': '_id',
#             'foreignField': 'authors',
#             'as': 'books'
#         }
#     },
#     {
#       '$addFields': {
#           'total_books': {'$size': '$books'}
#       }  
#     },
#     {
#         '$project': {'first_name': 1, 'last_name': 1, 'total_books': 1, '_id': 0}        
#     }
# ])
# printer.pprint(list(authors_book_count))




# ex. 4
# books_with_old_authors = library.book.aggregate([
#     {
#         '$lookup': {
#             'from': 'author',
#             'localField': 'authors',
#             'foreignField': '_id',
#             'as': 'authors'
#         }
#     },
#     {
#         '$set': {
#             'authors': {
#                 "$map": {
#                     'input': '$authors',
#                     'in': {
#                         'age': {
#                             '$dateDiff':{
#                                 'startDate': '$$this.date_of_birth',
#                                 'endDate': '$$NOW',
#                                 'unit': 'year'
#                             }
#                         },
#                         'first_name': '$$this.first_name',
#                         'last_name': '$$this.last_name',
#                     }
#                 }
#             }
#         }
#     },
#     {
#         '$match': {
#             '$and': [
#                 {'authors.age': {'$gte': 50}},
#                 {'authors.age': {'$lte': 150}},
#             ]
#         }
#     },
#     {
#         '$sort': {
#             'age': 1
#         }
#     }
# ])
# printer.pprint(list(books_with_old_authors))



# PyMongo Arrow Demo
import pyarrow
from pymongoarrow.api import Schema
from pymongoarrow.monkey import patch_all
import pymongoarrow as pma
from bson import ObjectId

patch_all()

author = Schema({'_id': ObjectId, 'first_name':pyarrow.string(), 'last_name': pyarrow.string(), 'date_of_birth': dt})
df = library.author.find_pandas_all({}, schema=author)
# print(df.head)
arrow_table = library.author.find_arrow_all({}, schema=author)
# print(arrow_table)
ndarrays = library.author.find_numpy_all({}, schema=author)
# print(ndarrays)
