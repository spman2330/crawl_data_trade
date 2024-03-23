import os
import sys
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
load_dotenv()

class MongoDB:
  HOST = os.environ.get("MONGODB_HOST", '0.0.0.0')
  PORT = os.environ.get("MONGODB_PORT", '8529')
  USERNAME = os.environ.get("MONGODB_USERNAME", "root")
  PASSWORD = os.environ.get("MONGODB_PASSWORD", "dev123")
  CONNECTION_URL = os.getenv("MONGODB_CONNECTION_URL") or f"mongodb@{USERNAME}:{PASSWORD}@http://{HOST}:{PORT}"
  def __init__(self) -> None:
    try:
      self.connection = MongoClient(self.CONNECTION_URL,
                                    connectTimeoutMS = 1000,
                                    serverSelectionTimeoutMS = 1000
                                    )
      print(f"Connect to MongoDB success")
    except Exception as e:
      print(f"Connect to MongoDB {e}")
      sys.exit(1)
  
  def update_data_list(self, database: str = None, collection: str = None, data: list = None):
    if not database or not collection:
      raise ConnectionError("Not specify database or collection")
    try:
      data_col = self.connection[database][collection]
      bulk_operations = [UpdateOne({"_id": item["_id"]}, {"$set": item}, upsert=True) for item in data]
      data_col.bulk_write(bulk_operations)
      print(f"update data to collection {collection} of database {database} sucesss")
    except Exception as e:
      print(f"update data to collection {collection} of database {database} {e}")

  
