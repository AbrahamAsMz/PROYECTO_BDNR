import json
from datetime import datetime
import pymongo
from cassandra.cluster import Cluster
import pydgraph

# ──────────────────────────────────────────────
#  MONGO DB
# ──────────────────────────────────────────────
mongo_client = pymongo.MongoClient("mongodb://localhost:27017")
mongo_db = mongo_client.learnlink

with open("data/mongo_data.json", "r", encoding="utf-8") as f:
    mongo_data = json.load(f)

# Insert users
if "users" in mongo_data:
    mongo_db.users.insert_many(mongo_data["users"])

# Insert courses
if "courses" in mongo_data:
    mongo_db.courses.insert_many(mongo_data["courses"])

# Insert lessons
if "lessons" in mongo_data:
    mongo_db.lessons.insert_many(mongo_data["lessons"])

# Insert enrollments
if "enrollments" in mongo_data:
    mongo_db.enrollments.insert_many(mongo_data["enrollments"])

# Insert reviews
if "reviews" in mongo_data:
    mongo_db.reviews.insert_many(mongo_data["reviews"])

print("Datos insertados en MongoDB correctamente.")


# ──────────────────────────────────────────────
#  CASSANDRA
# ──────────────────────────────────────────────
cluster = Cluster(["127.0.0.1"], port=9042)
session = cluster.connect("learnlink")

with open("data/cassandra_data.json", "r", encoding="utf-8") as f:
    cassandra_data = json.load(f)

# Poblar logging_info_by_user
for log in cassandra_data.get("logging_info_by_user", []):
    session.execute("""
        INSERT INTO logging_info_by_user (email, name, action, action_date)
        VALUES (%s, %s, %s, %s)
    """, (
        log["email"],
        log["name"],
        log["action"],
        datetime.fromisoformat(log["action_date"])
    ))

# Poblar course_info_by_status
for course in cassandra_data.get("course_info_by_status", []):
    grade = course.get("grade")

    # Evitar TypeError: NoneType → float
    if grade is None:
        grade = 0.0  # o -1 si quieres usarlo como "sin calificación"

    session.execute("""
        INSERT INTO course_info_by_status (course_title, email, name, status, grade)
        VALUES (%s, %s, %s, %s, %s)
    """, (
        course["course_title"],
        course["email"],
        course["name"],
        course["status"],
        float(grade)  # aseguramos floatcito precioso 💅
    ))


print("Datos insertados en Cassandra correctamente.")


# ──────────────────────────────────────────────
#  DGRAPH
# ──────────────────────────────────────────────
client_stub = pydgraph.DgraphClientStub('localhost:9080')
client = pydgraph.DgraphClient(client_stub)

# Cargar esquema
with open("Dgraph/schema.dql", "r", encoding="utf-8") as f:
    schema = f.read()

op = pydgraph.Operation(schema=schema)
client.alter(op)

# Cargar datos RDF
with open("data/dgraph_data.rdf", "r", encoding="utf-8") as f:
    rdf_data = f.read()

txn = client.txn()
try:
    txn.mutate(set_nquads=rdf_data, commit_now=True)  # ← AQUÍ VA EL FIX
    print("Datos insertados en Dgraph correctamente.")
finally:
    txn.discard()

print("\n\n TODAS LAS BASES DE DATOS FUERON POBLADAS EXITOSAMENTE")
