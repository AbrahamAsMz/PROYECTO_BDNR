import sys
import pandas as pd
import numpy as np
import json
from bson import ObjectId
from datetime import datetime
from connect import connect_mongo, connect_cassandra, connect_dgraph

# ---------------------------------------------------
# FUNCIONES UTILES
# ---------------------------------------------------

class CustomEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

def print_table(df):
    if df.empty:
        print("No hay registros")
    else:
        print(df.to_string(index=False))


# ------------------------------
# FUNCIONES PARA ACTUALIZAR DATA FILES
# ------------------------------

def update_mongo_file(collection_name, new_doc):
    data_file = "data/mongo_data.json"
    with open(data_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    if collection_name not in data:
        data[collection_name] = []
    data[collection_name].append(new_doc)
    with open(data_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, cls=CustomEncoder)

def update_cassandra_file(table_name, new_doc):
    data_file = "data/cassandra_data.json"
    with open(data_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    if table_name not in data:
        data[table_name] = []
    data[table_name].append(new_doc)
    with open(data_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, cls=CustomEncoder)

# ---------------------------------------------------
# LOGIN
# ---------------------------------------------------
def login():
    mongo = connect_mongo()
    print("=== Bienvenido a LearnLink ===")
    email = input("Email: ").strip()
    password = input("Password: ").strip()

    
    user = mongo.users.find_one({"email": email, "password": password})
    if not user:
        print("Email o contraseña incorrectos")
        return None
    print(f"¡Bienvenido, {user['name']}!")
    return user

# ---------------------------------------------------
# MENU PRINCIPAL POR ROL
# ---------------------------------------------------
def main_menu(user):
    role = user['role']
    if role == "admin":
        admin_menu(user)
    elif role == "instructor":
        instructor_menu(user)
    elif role == "student":
        student_menu(user)
    else:
        print("Rol no reconocido")
        sys.exit()

# ---------------------------------------------------
# MENU ADMIN
# ---------------------------------------------------
def admin_menu(user):
    while True:
        print("\n=== Menú Admin ===")
        print("1. Ver todos los usuarios")
        print("2. Consultar logs de todos los usuarios")
        print("3. Consultar logs de usuario específico")
        print("4. Cursos y calificaciones")
        print("5. Alumnos reprobados por curso")
        print("6. Contar alumnos por curso")
        print("7. Cursos por categoria")
        print("8. Probar conexiones a BD")
        print("9. Salir")
        choice = input("Elige una opción: ")
        
        if choice == "1":
            mostrar_usuarios()
        elif choice == "2":
            consultar_logs_todos()
        elif choice == "3":
            consultar_logs_usuario()
        elif choice == "4":
            consultar_calificaciones()
        elif choice == "5":
            alumnos_reprobados()
        elif choice == "6":
            contar_alumnos()
        elif choice == "7":
            cursos_por_categoria ()
        elif choice == "8":
            probar_conexiones()
        elif choice == "9":
            print("¡Hasta pronto!")
            sys.exit()
        else:
            print("Opción no válida.")

# ---------------------------------------------------
# MENU INSTRUCTOR
# ---------------------------------------------------
def instructor_menu(user):
    while True:
        print(f"\n=== Menú Instructor ({user['name']}) ===")
        print("1. Ver cursos que imparto")
        print("2. Ver calificaciones de mis alumnos por curso")
        print("3. Ver alumnos inscritos en un curso")
        print("4. Contar lecciones por curso")
        print("5. Salir")
        choice = input("Elige una opción: ")

        if choice == "1":
            cursos_instructor(user)
        elif choice == "2":
            calificaciones_curso(user)
        elif choice == "3":
            alumnos_curso(user)
        elif choice == "4":
            contar_lecciones_curso ()
        elif choice == "5":
            print("¡Hasta pronto!")
            sys.exit()
        else:
            print("Opción no válida.")
            
        

# ---------------------------------------------------
# MENU ALUMNO
# ---------------------------------------------------
def student_menu(user):
    while True:
        print(f"\n=== Menú Alumno ({user['name']}) ===")
        print("1. Ver cursos inscritos")
        print("2. Ver calificaciones")
        print("3. Ver cursos pendientes")
        print("4. Inscribirse en un curso")
        print("5. Escribir reseña")
        print("6. Ver mis reseñas")
        print("7. Salir")
        choice = input("Elige una opción: ")

        if choice == "1":
            mis_cursos(user)
        elif choice == "2":
            mis_calificaciones(user)
        elif choice == "3":
            cursos_pendientes(user)
        elif choice == "4":
            inscribirse_curso(user)
        elif choice == "5":
            escribir_reseña(user)
        elif choice == "6":
            ver_mis_reseñas(user)
        elif choice == "7":
            print("¡Hasta pronto!")
            sys.exit()
        else:
            print("Opción no válida.")

# ---------------------------------------------------
# FUNCIONES ADMIN
# ---------------------------------------------------
def mostrar_usuarios():
    mongo = connect_mongo()
    usuarios = list(mongo.users.find())
    df = pd.DataFrame(usuarios)
    print_table(df[["name", "email", "role"]])

def consultar_logs_todos():
    cass = connect_cassandra()
    query = "SELECT email, action, action_date FROM logging_info_by_user"
    rows = list(cass.execute(query))
    df = pd.DataFrame(rows)
    print_table(df)

def consultar_logs_usuario():
    cass = connect_cassandra()
    email = input("Email del usuario: ").strip()
    query = f"SELECT email, action, action_date FROM logging_info_by_user WHERE email='{email}'"
    rows = list(cass.execute(query))
    df = pd.DataFrame(rows)
    print_table(df)

def consultar_calificaciones():
    cass = connect_cassandra()
    course_title = input("Nombre del curso: ").strip()
    query = f"SELECT course_title, name, email, grade FROM course_info_by_status WHERE status='completed' AND course_title='{course_title}' ALLOW FILTERING"
    rows = list(cass.execute(query))
    df = pd.DataFrame(rows)
    print_table(df)

def alumnos_reprobados():
    cass = connect_cassandra()
    course_title = input("Curso: ").strip()
    query = f"SELECT name, email, grade FROM course_info_by_status WHERE status='completed' AND course_title='{course_title}' ALLOW FILTERING"
    rows = list(cass.execute(query))
    df = pd.DataFrame(rows)

    # Convertir grade a número para que el filtro funcione
    df['grade'] = pd.to_numeric(df['grade'], errors='coerce')
    
    reprobados = df[df['grade'] < 6]
    print_table(reprobados)
    print(f"Total reprobados: {len(reprobados)}")

def contar_alumnos():
    cass = connect_cassandra()
    course_title = input("Curso: ").strip()
    query = f"SELECT COUNT(*) FROM course_info_by_status WHERE status='active' AND course_title='{course_title}' ALLOW FILTERING"
    rows = list(cass.execute(query))
    print(f"Alumnos tomando '{course_title}': {rows[0][0]}")
    
def cursos_por_categoria():
    mongo = connect_mongo()
    pipeline = [
        {"$group": {"_id": "$category", "total_courses": {"$sum": 1}, "courses": {"$push": "$title"}}}
    ]
    result = list(mongo.courses.aggregate(pipeline))
    for r in result:
        print(f"Categoría: {r['_id']} | Cursos: {r['total_courses']}")
        for c in r['courses']:
            print(f"  - {c}")

def probar_conexiones():
    mongo = connect_mongo()
    cass = connect_cassandra()
    dgraph = connect_dgraph()
    print("MongoDB OK")
    print("Cassandra OK")
    print("Dgraph OK")

# ---------------------------------------------------
# FUNCIONES INSTRUCTOR
# ---------------------------------------------------
def cursos_instructor(user):
    mongo = connect_mongo()
    cursos = list(mongo.courses.find({"instructor_email": user['email']}))
    df = pd.DataFrame(cursos)
    print_table(df[["title","category"]])

def calificaciones_curso(user):
    cass = connect_cassandra()
    course_title = input("Curso: ").strip()
    query = f"SELECT name, email, grade FROM course_info_by_status WHERE status='completed' AND course_title='{course_title}' ALLOW FILTERING"
    rows = list(cass.execute(query))
    df = pd.DataFrame(rows)
    print_table(df)

def alumnos_curso(user):
    cass = connect_cassandra()
    course_title = input("Curso: ").strip()
    query = f"SELECT name, email FROM course_info_by_status WHERE status='active' AND course_title='{course_title}' ALLOW FILTERING"
    rows = list(cass.execute(query))
    df = pd.DataFrame(rows)
    print_table(df)
    
def contar_lecciones_curso():
    mongo = connect_mongo()  # <-- Conexión a MongoDB
    course_title = input("Ingresa el nombre del curso: ").strip()
    pipeline = [
        {"$match": {"course_title": course_title}},
        {"$count": "total_lessons"}
    ]
    result = list(mongo.lessons.aggregate(pipeline))  # <-- usar mongo
    if result:
        print(f"El curso '{course_title}' tiene {result[0]['total_lessons']} lecciones.")
    else:
        print(f"No se encontraron lecciones para el curso '{course_title}'.")



# ---------------------------------------------------
# FUNCIONES ALUMNO
# ---------------------------------------------------
def mis_cursos(user):
    mongo = connect_mongo()
    cursos = list(mongo.enrollments.find({"user_email": user['email']}))
    df = pd.DataFrame(cursos)
    print_table(df[["course_title","enroll_date"]])

def mis_calificaciones(user):
    cass = connect_cassandra()
    query = f"SELECT course_title, grade FROM course_info_by_status WHERE email='{user['email']}' AND status='completed'"
    rows = list(cass.execute(query))
    df = pd.DataFrame(rows)
    print_table(df)

def cursos_pendientes(user):
    cass = connect_cassandra()
    query = f"SELECT course_title FROM course_info_by_status WHERE email='{user['email']}' AND status='active'"
    rows = list(cass.execute(query))
    df = pd.DataFrame(rows)
    print_table(df)

def inscribirse_curso(user):
    mongo = connect_mongo()
    cass = connect_cassandra()
    dgraph = connect_dgraph()
    course_title = input("Curso a inscribirse: ").strip()
    enroll_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # MongoDB
    mongo_doc = {"user_email": user['email'], "course_title": course_title, "enroll_date": enroll_date}
    mongo.enrollments.insert_one(mongo_doc)
    update_mongo_file("enrollments", mongo_doc)
    
    # Cassandra
    cass_doc = {"course_title": course_title, "email": user['email'], "name": user['name'], "status": "active", "grade": None}
    query = f"""
    INSERT INTO course_info_by_status (course_title, email, name, status, grade) 
    VALUES ('{course_title}', '{user['email']}', '{user['name']}', 'active', null)
    """
    cass.execute(query)
    update_cassandra_file("course_info_by_status", cass_doc)
    
    # Dgraph (placeholder)
    # dgraph.txn().mutate(...)
    
    print(f"Te has inscrito al curso '{course_title}' correctamente")

def escribir_reseña(user):
    mongo = connect_mongo()
    course_title = input("Curso: ").strip()
    comment = input("Comentario: ").strip()
    rating = int(input("Rating (1-5): "))
    
    review_doc = {"course_title": course_title, "username": user['name'], "comment": comment, "rating": rating}
    
    # Insertar en Mongo
    mongo.reviews.insert_one(review_doc)
    update_mongo_file("reviews", review_doc)
    
    print("Reseña agregada")


def ver_mis_reseñas(user):
    mongo = connect_mongo()
    reviews = list(mongo.reviews.find({"username": user['name']}))
    df = pd.DataFrame(reviews)
    print_table(df[["course_title","comment","rating"]])
    print(f"Total reseñas: {len(df)}")




def promedio_reseñas_curso():
    course_title = input("Ingresa el nombre del curso: ").strip()
    pipeline = [
        {"$match": {"course_title": course_title}},
        {"$group": {"_id": "$course_title", "avg_rating": {"$avg": "$rating"}}}
    ]
    result = list(db.reviews.aggregate(pipeline))
    if result:
        print(f"El promedio de reseñas del curso '{course_title}' es {result[0]['avg_rating']:.2f}")
    else:
        print(f"No se encontraron reseñas para el curso '{course_title}'.")

def reseñas_por_alumno():
    username = input("Ingresa el nombre del alumno: ").strip()
    pipeline = [
        {"$match": {"username": username}},
        {"$group": {"_id": "$username", "total_reseñas": {"$sum": 1}, "detalles": {"$push": {"course_title": "$course_title", "comment": "$comment", "rating": "$rating"}}}}
    ]
    result = list(db.reviews.aggregate(pipeline))
    if result:
        print(f"El alumno '{username}' ha hecho {result[0]['total_reseñas']} reseñas:")
        for r in result[0]["detalles"]:
            print(f"- Curso: {r['course_title']}, Rating: {r['rating']}, Comentario: {r['comment']}")
    else:
        print(f"No se encontraron reseñas para el alumno '{username}'.")

def cursos_por_alumno():
    email = input("Ingresa el email del alumno: ").strip()
    pipeline = [
        {"$match": {"user_email": email}},
        {"$project": {"_id": 0, "course_title": 1, "enroll_date": 1}}
    ]
    result = list(db.enrollments.aggregate(pipeline))
    if result:
        print(f"El alumno '{email}' está inscrito en estos cursos:")
        for r in result:
            print(f"- {r['course_title']}, Fecha de inscripción: {r['enroll_date']}")
    else:
        print(f"No se encontraron cursos para el alumno '{email}'.")

def cursos_por_instructor():
    email = input("Ingresa el email del instructor: ").strip()
    pipeline = [
        {"$match": {"instructor_email": email}},
        {"$project": {"_id": 0, "title": 1, "category": 1}}
    ]
    result = list(db.courses.aggregate(pipeline))
    if result:
        print(f"El instructor '{email}' imparte los siguientes cursos:")
        for r in result:
            print(f"- {r['title']}, Categoría: {r['category']}")
    else:
        print(f"No se encontraron cursos para el instructor '{email}'.")
# ---------------------------------------------------
# ARRANQUE
# ---------------------------------------------------
if __name__ == "__main__":
    user = None
    while not user:
        user = login()
    main_menu(user)

