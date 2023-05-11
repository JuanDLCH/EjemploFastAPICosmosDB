from fastapi import FastAPI
from pymongo import MongoClient
from bson.objectid import ObjectId
from pymongo import ASCENDING
from pymongo.errors import OperationFailure

app = FastAPI()

# String de conexión a CosmosDB
cosmosdb_conn_str = "mongodb://josefabio:7KXKi5h2RBAzQqvMTwJfT0bRMzYl9nOL01ngkaWYlwGAQfIJ31s5JQxFU2l8Y5WXIRYYPUpKr5nOACDbJghIxw==@josefabio.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@josefabio@"

# Conexión a la base de datos
client = MongoClient(cosmosdb_conn_str)
db = client["SampleDB"]
collection = db["SampleCollection"]


@app.get("/")
async def get_all():
    results = []
    for doc in collection.find():
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return {"data": results}


@app.get("/get_by_id/{id}")
async def get_by_id(id: str):
    result = collection.find_one({"_id": ObjectId(id)})
    if result:
        result["_id"] = str(result["_id"])
        return {"data": result}
    else:
        return {"data": "Not found"}


@app.get("/get_by_name/{name}")
async def get_by_name(name: str):
    regex = f".*{name}.*"  # Crear expresión regular que contenga el nombre
    results = []
    for doc in collection.find({"name": {"$regex": regex}}):
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return {"data": results}


@app.get("/get_categories")
async def get_categories():
    try:
        pipeline = [
            {"$project": {"categoryName": {"$split": ["$categoryName", ","]}}},
            {"$unwind": "$categoryName"},
            {"$project": {"categoryName": {"$trim": {"input": "$categoryName"}}}},
            {"$group": {"_id": "$categoryName"}},
            {"$sort": {"_id": ASCENDING}}
        ]
        result = collection.aggregate(pipeline)
        categories = [doc["_id"] for doc in result]
        return {"categories": categories}
    except OperationFailure as e:
        print("Error al ejecutar la consulta de agregación:", e.details)
        return {"categories": []}


@app.post("/insert")
async def insert(data: dict):
    result = collection.insert_one(data)
    return {"inserted_id": str(result.inserted_id)}


@app.put("/update/{id}")
async def update(id: str, data: dict):
    result = collection.update_one({"_id": ObjectId(id)}, {"$set": data})
    return {"modified_count": result.modified_count}


@app.delete("/delete/{id}")
async def delete(id: str):
    result = collection.delete_one({"_id": ObjectId(id)})
    return {"deleted_count": result.deleted_count}
