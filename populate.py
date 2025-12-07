import json
import pymongo
import pydgraph
import uuid
import time
from datetime import datetime
from cassandra.cluster import Cluster

# --- RUTAS A LOS ARCHIVOS ---
MONGO_DATA_FILE = "data/mongo_data.json"
CASSANDRA_DATA_FILE = "data/cassandra_data.json"
DGRAPH_SCHEMA_FILE = "Dgraph/schema.dql" 
DGRAPH_DATA_FILE = "data/dgraph_data.rdf"

print("Iniciando el proceso de población de bases de datos...")

# ###############################################################
#  1. MONGO DB
# ###############################################################
try:
    print("\n--- Conectando a MongoDB (127.0.0.1:27017)...")
    mongo_client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=5000)
    mongo_db = mongo_client.learnlink
    mongo_client.server_info()
    print("MongoDB conectado.")

    with open(MONGO_DATA_FILE, "r", encoding="utf-8") as f:
        mongo_data = json.load(f)

    print("Limpiando colecciones...")
    mongo_db.users.delete_many({})
    mongo_db.courses.delete_many({})
    mongo_db.lessons.delete_many({})
    mongo_db.enrollments.delete_many({})
    mongo_db.reviews.delete_many({})

    # --- CREACIÓN DE ÍNDICES (NUEVO) ---
    print("Creando índices en MongoDB...")
    
    mongo_db.users.create_index([("email", pymongo.ASCENDING)], unique=True)
    mongo_db.users.create_index([("user_uuid", pymongo.ASCENDING)], unique=True)

    mongo_db.courses.create_index([("title", pymongo.TEXT), ("category", pymongo.TEXT)])
    mongo_db.courses.create_index([("course_uuid", pymongo.ASCENDING)], unique=True)

    mongo_db.lessons.create_index([("title", pymongo.TEXT)])
    mongo_db.lessons.create_index([("course_title", pymongo.ASCENDING)])

    mongo_db.enrollments.create_index([("user_email", pymongo.ASCENDING), ("course_title", pymongo.ASCENDING)], unique=True)

    mongo_db.reviews.create_index([("course_title", pymongo.ASCENDING), ("username", pymongo.ASCENDING)])
    print("Índices creados.")

    print("Insertando datos en MongoDB...")
    if "users" in mongo_data: mongo_db.users.insert_many(mongo_data["users"])
    if "courses" in mongo_data: mongo_db.courses.insert_many(mongo_data["courses"])
    if "lessons" in mongo_data: mongo_db.lessons.insert_many(mongo_data["lessons"])
    if "enrollments" in mongo_data: mongo_db.enrollments.insert_many(mongo_data["enrollments"])
    if "reviews" in mongo_data: mongo_db.reviews.insert_many(mongo_data["reviews"])

    print("MongoDB: OK.")

except Exception as e:
    print(f"ERROR AL POBLAR MONGODB: {e}")

# ###############################################################
#  2. CASSANDRA
# ###############################################################
try:
    print("\n--- Conectando a Cassandra (127.0.0.1:9042)...")
    cluster = Cluster(["127.0.0.1"], port=9042)
    session = cluster.connect()
    
    # 1. Configurar Keyspace
    print("Configurando Keyspace 'learnlink'...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS learnlink 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace("learnlink")

    print("Recreando tablas en Cassandra...")
    
    tablas = ["logs_by_user", "logs_by_role", "student_portfolio", "course_activity"]
    for t in tablas:
        try:
            session.execute(f"DROP TABLE IF EXISTS {t}")
        except Exception:
            pass

    session.execute("""
        CREATE TABLE logs_by_user (
            email TEXT,
            action TEXT,
            action_date TIMESTAMP,
            user_id UUID,
            name TEXT,
            role TEXT,
            PRIMARY KEY ((email), action, action_date)
        ) WITH CLUSTERING ORDER BY (action ASC, action_date DESC)
    """)

    session.execute("""
        CREATE TABLE logs_by_role (
            role TEXT,
            email TEXT,
            action_date TIMESTAMP,
            name TEXT,
            action TEXT,
            user_id UUID,
            PRIMARY KEY ((role), email, action_date, action)
        ) WITH CLUSTERING ORDER BY (email ASC, action_date DESC, action ASC)
    """)

    session.execute("""
        CREATE TABLE student_portfolio (
            email TEXT,
            status TEXT,
            course_title TEXT,
            grade FLOAT,
            course_id UUID,
            user_id UUID,
            name TEXT,
            PRIMARY KEY ((email), status, course_title)
        )
    """)

    session.execute("""
        CREATE TABLE course_activity (
            course_title TEXT,
            status TEXT,
            grade FLOAT,
            email TEXT,
            name TEXT,
            course_id UUID,
            user_id UUID,
            PRIMARY KEY ((course_title), status, grade, email)
        )
    """)

    with open(CASSANDRA_DATA_FILE, "r", encoding="utf-8") as f:
        cassandra_data = json.load(f)

    print("Preparando inserts...")
    q_logs_user = session.prepare("INSERT INTO logs_by_user (email, action, action_date, user_id, name, role) VALUES (?, ?, ?, ?, ?, ?)")
    q_logs_role = session.prepare("INSERT INTO logs_by_role (role, email, action_date, name, action, user_id) VALUES (?, ?, ?, ?, ?, ?)")
    q_student = session.prepare("INSERT INTO student_portfolio (email, status, course_title, grade, course_id, user_id, name) VALUES (?, ?, ?, ?, ?, ?, ?)")
    q_course = session.prepare("INSERT INTO course_activity (course_title, status, grade, email, name, course_id, user_id) VALUES (?, ?, ?, ?, ?, ?, ?)")

    print("Insertando Logs...")
    raw_logs = cassandra_data.get("logging_info_by_email", [])
    for log in raw_logs:
        role = log.get("role", "student")
        uid = uuid.UUID(log["user_id"])
        dt = datetime.fromisoformat(log["action_date"])
        
        session.execute(q_logs_user, (log["email"], log["action"], dt, uid, log["name"], role))
        session.execute(q_logs_role, (role, log["email"], dt, log["name"], log["action"], uid))

    print("Insertando Cursos...")
    raw_courses = cassandra_data.get("course_info_by_status", [])
    for c in raw_courses:
        grade = float(c["grade"]) if c.get("grade") is not None else 0.0
        uid = uuid.UUID(c["user_id"])
        cid = uuid.UUID(c["course_id"])
        
        session.execute(q_student, (c["email"], c["status"], c["course_title"], grade, cid, uid, c["name"]))
        session.execute(q_course, (c["course_title"], c["status"], grade, c["email"], c["name"], cid, uid))

    print("Cassandra: OK.")

except Exception as e:
    print(f"ERROR AL POBLAR CASSANDRA: {e}")

# ###############################################################
#  3. DGRAPH
# ###############################################################
try:
    print("\n--- Conectando a Dgraph (127.0.0.1:9080)...")
    client_stub = pydgraph.DgraphClientStub('127.0.0.1:9080')
    client = pydgraph.DgraphClient(client_stub)
    
    try:
        client.check_version()
        print("Dgraph conectado.")
    except Exception as e:
        print(f"Advertencia Dgraph: {e}")

    with open(DGRAPH_SCHEMA_FILE, "r", encoding="utf-8") as f:
        schema = f.read()
    op_schema = pydgraph.Operation(schema=schema)
    client.alter(op_schema)
    print("Esquema Dgraph cargado.")

    with open(DGRAPH_DATA_FILE, "r", encoding="utf-8") as f:
        rdf_data = f.read()

    txn = client.txn()
    try:
        txn.mutate(set_nquads=rdf_data, commit_now=True)
        print("Dgraph: OK.")
    except Exception as e:
        print(f"Error insertando datos Dgraph: {e}")
    finally:
        txn.discard()

except Exception as e:
    print(f"ERROR AL POBLAR DGRAPH: {e}")

print("\n\n" + "="*80)
print(" PROCESO TERMINADO ".center(80))
print("="*80)