import sys
import os
import json
import uuid  
import hashlib 
import requests
import pandas as pd
import numpy as np
import time 
from bson import ObjectId
from datetime import datetime
from tabulate import tabulate
from connect import connect_mongo, connect_cassandra, connect_dgraph 

#################################################################
# SECCIÓN 1: UTILIDADES GENERALES
#################################################################

def clear_screen():
    """ Limpia la pantalla de la consola. """
    os.system('cls' if os.name == 'nt' else 'clear')

def hash_password(password):
    """ Hashea una contraseña usando SHA-256. """
    return hashlib.sha256(password.encode()).hexdigest()

def press_enter_to_continue():
    """ Pausa el programa hasta que el usuario presione Enter. """
    input("\n\nPresiona Enter para regresar al menú...")
    clear_screen()

def print_helper_table(data, headers, title="OPCIONES DISPONIBLES"):
    """ Función auxiliar para imprimir tablas de selección de datos. """
    if not data:
        print(f"\n[No hay datos disponibles para: {title}]")
        return
    print("\n" + f"--- {title} ---".center(80))
    print(tabulate(data, headers=headers, tablefmt="fancy_grid", showindex=False))
    print("-" * 80 + "\n")


#################################################################
# SECCIÓN 2: FUNCIONES DE DGRAPH (API)
#################################################################

def dgraph_run_query(query):
    """ Función genérica para ejecutar cualquier consulta DQL (Query). """
    try:
        res = requests.post(
            "http://127.0.0.1:8080/query",
            data=query.encode("utf-8"),
            headers={"Content-Type": "application/graphql+-"}
        )
        res.raise_for_status()
        return res.json()
    except requests.exceptions.RequestException as e:
        print(f"Error de conexión con Dgraph: {e}")
        return None
    except json.JSONDecodeError:
        print("Error: Dgraph no devolvió un JSON válido.")
        return None

def dgraph_run_mutate(mutation_rdf):
    """ Función genérica para ejecutar cualquier mutación RDF en Dgraph. """
    try:
        res = requests.post(
            "http://127.0.0.1:8080/mutate?commitNow=true",
            data=mutation_rdf.encode("utf-8"),
            headers={"Content-Type": "application/rdf"}
        )
        res.raise_for_status()
        data = res.json()
        
        if data.get("errors"):
            print(f"Error en la mutación de Dgraph: {data['errors'][0]['message']}")
            return None
        
        return data.get("data", {}).get("uids", {})
        
    except requests.exceptions.RequestException as e:
        print(f"Error de conexión con Dgraph (mutación): {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error al parsear la respuesta de Dgraph (mutación): {e}", res.text)
        return None

def dgraph_get_uid_by_email(email):
    """ Busca el UID de un User o Instructor en Dgraph usando su email. """
    query = f"""
    {{
      user(func: eq(email, "{email}")) {{
        uid
      }}
    }}
    """
    data = dgraph_run_query(query)
    if not data:
        return None
    users = data.get("data", {}).get("user", [])
    return users[0]["uid"] if users else None

def dgraph_get_uid_by_title(title):
    """ Busca el UID de un Curso en Dgraph usando su título. """
    q = f'''
    {{
      course(func: eq(title, "{title}")) {{
        uid
      }}
    }}
    '''
    data = dgraph_run_query(q)
    if not data:
        return None
    courses = data.get("data", {}).get("course", [])
    return courses[0]["uid"] if courses else None

def dgraph_insert_enrollment(user_uid, course_uid, enroll_date):
    """ Inserta una nueva matrícula en Dgraph. """
    safe_date = enroll_date.replace('"', "'")
    mutation = f"""
    {{
      set {{
        _:newenroll <dgraph.type> "Enrollment" .
        _:newenroll <status> "active" .
        _:newenroll <of_course> <{course_uid}> .
        _:newenroll <enroll_date> "{safe_date}" .
        <{user_uid}> <enrolled_in> _:newenroll .
      }}
    }}
    """
    uids = dgraph_run_mutate(mutation)
    if uids:
        return uids.get("newenroll")
    return None

def dgraph_insert_review(comment, rating, user_uid, course_uid):
    """ Inserta una nueva reseña en Dgraph. """
    safe_comment = comment.replace('"', "'")
    mutation = f"""
    {{
      set {{
        _:newreview <dgraph.type> "Review" .
        _:newreview <comment> "{safe_comment}" .
        _:newreview <rating> "{float(rating)}" .
        _:newreview <review_of> <{course_uid}> .
        _:newreview <reviewed_by> <{user_uid}> .
      }}
    }}
    """
    uids = dgraph_run_mutate(mutation)
    if uids:
        return uids.get("newreview")
    return None


#################################################################
# SECCIÓN 3: LÓGICA DE SESIÓN (LOGIN/LOGOUT)
#################################################################

def login(mongo, cass):
    """ Maneja el proceso de login del usuario. """
    clear_screen()
    print("="*80)
    print("BIENVENIDO A LEARNLINK".center(80))
    print("="*80)
    email = input("\nEmail: ").strip()
    password = input("Password: ").strip()

    try:
        hashed_pass = hash_password(password)
        user = mongo.users.find_one({"email": email, "password": hashed_pass})
    except Exception as e:
        print(f"Error al consultar Mongo: {e}")
        return None
        
    if not user:
        user = mongo.users.find_one({"email": email, "password": password})
        if not user:
            print("\nEmail o contraseña incorrectos")
            return None

    action_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    user_uuid = user.get("user_uuid")
    role = user.get("role", "student")
    
    if user_uuid:
        try:
            q1 = """
            INSERT INTO logs_by_user (email, action, action_date, user_id, name, role)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cass.execute(q1, (email, 'log_in', action_date, uuid.UUID(user_uuid), user['name'], role))

            q2 = """
            INSERT INTO logs_by_role (role, email, action_date, name, action, user_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cass.execute(q2, (role, email, action_date, user['name'], 'log_in', uuid.UUID(user_uuid)))

        except Exception as e:
            print(f"\nADVERTENCIA: Login exitoso, pero falló el registro en Cassandra: {e}")
            pass

    clear_screen()
    return user

def logout(user, cass):
    """ Registra el logout en Cassandra y termina el programa. """
    try:
        logout_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        user_uuid = user.get("user_uuid")
        role = user.get("role", "student")
        
        if user_uuid:
            q1 = """
            INSERT INTO logs_by_user (email, action, action_date, user_id, name, role)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cass.execute(q1, (user['email'], 'log_out', logout_date, uuid.UUID(user_uuid), user['name'], role))

            q2 = """
            INSERT INTO logs_by_role (role, email, action_date, name, action, user_id)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cass.execute(q2, (role, user['email'], logout_date, user['name'], 'log_out', uuid.UUID(user_uuid)))

    except Exception as e:
        print(f"ADVERTENCIA: Falló el registro de logout en Cassandra: {e}")
    finally:
        print("="*80)
        print("SESION FINALIZADA".center(80))
        print("="*80)
        sys.exit()


#################################################################
# SECCIÓN 4: MENÚS DE NAVEGACIÓN
#################################################################

def main_menu(user, mongo, cass):
    role = user.get('role')
    if role == "admin":
        admin_menu(user, mongo, cass)
    elif role == "instructor":
        instructor_menu(user, mongo, cass)
    elif role == "student":
        student_menu(user, mongo, cass)
    else:
        print(f"Rol '{role}' no reconocido")
        sys.exit()

def admin_menu(user, mongo, cass):
    while True:
        menu_items = [
            ["Sección de Administración"],
            ["1", "Registrar Nuevo Usuario (M1)"],
            ["2", "Crear Nuevo Curso (M2)"],
            ["3", "Añadir Lección a Curso (M3)"],
            ["4", "Buscar Usuarios por Rol (M7)"],
            ["5", "Buscar Lección (M8)"],
            ["6", "Ver Reseñas por Curso (M6)"],
            ["7", "Consultar logs por ROL (M2) "],
            ["8", "Consultar logs de usuario específico (C3)"],
            ["9", "Calificaciones de alumnos por curso (C6)"],
            ["10", "Alumnos reprobados por curso (C9)"],
            ["11", "Contar alumnos activos por curso (C10)"],
            ["12", "Reportes de Grafo (D1-D12)"],
            ["13", "Probar conexiones a BD"],
            ["14", "Salir"]
        ]
        print(f"\n===== Menú Admin =====\n")
        print(tabulate(menu_items, tablefmt="fancy_grid"))
        choice = input("\nElige una opción: ")
        clear_screen()

        if choice == "1": admin_registrar_usuario(mongo)
        elif choice == "2": admin_crear_curso(mongo)
        elif choice == "3": admin_anadir_leccion(mongo)
        elif choice == "4": admin_buscar_usuarios_por_rol(mongo)
        elif choice == "5": admin_buscar_leccion(mongo)
        elif choice == "6": admin_ver_reseñas_por_curso(mongo)
        elif choice == "7": consultar_logs_todos(cass)
        elif choice == "8": consultar_logs_usuario(cass)
        elif choice == "9": consultar_calificaciones(cass, mongo)
        elif choice == "10": alumnos_reprobados(cass, mongo)
        elif choice == "11": contar_alumnos(cass, mongo)
        elif choice == "12": menu_reportes_dgraph(user) 
        elif choice == "13": probar_conexiones(mongo, cass)
        elif choice == "14": logout(user, cass)
        else: print("\nOpción no válida")

def instructor_menu(user, mongo, cass):
    while True:
        menu_items = [
            ["1", "Ver cursos que imparto (M11)"],
            ["2", "Añadir lección a un curso (M3)"],
            ["3", "Ver calificaciones de mis alumnos por curso (C6)"],
            ["4", "Ver alumnos en un curso (C11)"],
            ["5", "Contar lecciones por curso (M12)"],
            ["6", "Salir"]
        ]
        print(f"\n===== Menú del instructor {user['name']} =====\n")
        print(tabulate(menu_items, headers=["Opción", "Descripción"], tablefmt="fancy_grid"))
        choice = input("\nElige una opción: ")
        clear_screen()

        if choice == "1": cursos_instructor(user, mongo)
        elif choice == "2": instructor_anadir_leccion(user, mongo)
        elif choice == "3": calificaciones_curso(user, mongo, cass)
        elif choice == "4": alumnos_curso(user, mongo, cass)
        elif choice == "5": contar_lecciones_curso(mongo)
        elif choice == "6": logout(user, cass)
        else: print("\nOpción no válida")

def student_menu(user, mongo, cass):
    while True:
        menu_items = [
            ["1", "Ver cardex (registro historico) (M9)"],
            ["2", "Ver mis calificaciones (C7)"],
            ["3", "Ver cursos pendientes (C8)"],
            ["4", "Ver mi historial de sesión (C4, C5)"],
            ["5", "Inscribirse en un curso (M4)"],
            ["6", "Escribir reseña (M5)"],
            ["7", "Ver mis reseñas (M10)"],
            ["8", "Recomendaciones de Cursos (D4)"],
            ["9", "Ver compañeros de mis instructores (D8)"],
            ["10", "Salir"]
        ]
        print(f"\n===== Menú del alumno {user['name']} =====\n\n")
        print(tabulate(menu_items, headers=["Opción", "Descripción"], tablefmt="fancy_grid"))
        choice = input("\nElige una opción: ")
        clear_screen()

        if choice == "1": mis_cursos(user, mongo)
        elif choice == "2": mis_calificaciones(user, cass)
        elif choice == "3": cursos_pendientes(user, cass)
        elif choice == "4": ver_mi_historial_sesion(user, cass)
        elif choice == "5": inscribirse_curso(user, mongo, cass)
        elif choice == "6": escribir_reseña(user, mongo)
        elif choice == "7": ver_mis_reseñas(user, mongo)
        elif choice == "8": 
            dgraph_report_D4(user, is_student_mode=True)
            press_enter_to_continue()
        elif choice == "9":
            dgraph_report_D8(user, is_student_mode=True)
            press_enter_to_continue()
        elif choice == "10": logout(user, cass)
        else: print("\nOpción no válida.")


#################################################################
# SECCIÓN 5: FUNCIONES DE ALUMNO
#################################################################

def mis_cursos(user, mongo):
    cursos = list(mongo.enrollments.find({"user_email": user['email']}))
    print("\n" + "="*80 + "\n" + "MIS CURSOS".center(80) + "\n" + "="*80)
    if not cursos:
        print("\nNo estás inscrito en ningún curso")
    else:
        table_data = [[c["course_title"], c["enroll_date"]] for c in cursos]
        print("\nTus cursos inscritos:")
        print(tabulate(table_data, headers=["Curso", "Fecha de inscripción"], tablefmt="fancy_grid"))
    press_enter_to_continue()

def mis_calificaciones(user, cass):
    """ (C7) Muestra las calificaciones de cursos completados """
    print("\n" + "="*80 + "\n" + "MIS CALIFICACIONES".center(80) + "\n" + "="*80)
    
    query = "SELECT course_title, grade FROM student_portfolio WHERE email=%s AND status='completed'"
    
    try:
        rows = list(cass.execute(query, (user['email'],)))
        
        if not rows:
            print("\nNo tienes calificaciones registradas (o no tienes cursos en estado 'completed').")
        else:
            table_data = [[r.course_title, r.grade] for r in rows]
            print("\nTus calificaciones:")
            print(tabulate(table_data, headers=["Curso", "Calificación"], tablefmt="fancy_grid"))
    except Exception as e:
        print(f"\nError al consultar Cassandra: {e}")
    press_enter_to_continue()

def cursos_pendientes(user, cass):
    """ (C8) Muestra los cursos activos  """
    print("\n" + "="*80 + "\n" + "CURSOS ACTIVOS".center(80) + "\n" + "="*80)
    
    query = "SELECT course_title FROM student_portfolio WHERE email=%s AND status='active'"
    
    try:
        rows = list(cass.execute(query, (user['email'],)))
        if not rows:
            print("\nNo tienes cursos activos actualmente.")
        else:
            data = [{"Curso": r.course_title} for r in rows]
            print("\n" + tabulate(data, headers="keys", tablefmt="fancy_grid", showindex=False))
    except Exception as e:
        print(f"\nError al consultar Cassandra: {e}")
    press_enter_to_continue()

def ver_mi_historial_sesion(user, cass):
    """ (C4, C5) Muestra el historial de sesión del usuario """
    email = user['email']
    print("\n" + "="*80 + "\n" + "MI HISTORIAL DE SESIÓN".center(80) + "\n" + "="*80)
    
    filtro = input("Filtrar por (deja en blanco para 'todos', 'log_in' o 'log_out'): ").strip().lower()
    
    try:
        if filtro == 'log_in' or filtro == 'log_out':
            query = "SELECT action, action_date FROM logs_by_user WHERE email=%s AND action=%s"
            rows = list(cass.execute(query, (email, filtro)))
        else:
            query = "SELECT action, action_date FROM logs_by_user WHERE email=%s"
            rows = list(cass.execute(query, (email,)))
            
        if not rows:
            print(f"\nNo se encontraron registros para {email}.")
        else:
            table_data = [[r.action, r.action_date] for r in rows]
            print(tabulate(table_data, headers=["Acción", "Fecha/Hora"], tablefmt="fancy_grid"))
    except Exception as e:
        print(f"\nError leyendo logs: {e}")
        
    press_enter_to_continue()

def inscribirse_curso(user, mongo, cass):
    email = user["email"]
    user_uuid = user.get("user_uuid")
    if not user_uuid:
        print("Error: Tu cuenta de usuario no tiene un UUID.")
        press_enter_to_continue()
        return

    print("\n" + "="*80 + "\n" + "INSCRIPCIÓN A UN CURSO".center(80) + "\n" + "="*80)
    
    cursos = list(mongo.courses.find())
    if not cursos:
        print("No hay cursos disponibles.")
        press_enter_to_continue()
        return

    table_cursos = [[c["title"].strip()] for c in cursos]
    print(tabulate(table_cursos, headers=["Cursos disponibles"], tablefmt="fancy_grid", showindex=False))

    course_title = input("\nIngresa el nombre del curso: ").strip()
    if not course_title:
        print("\nInscripción cancelada.")
        press_enter_to_continue()
        return
        
    curso_mongo = mongo.courses.find_one({"title": course_title})
    if not curso_mongo:
        print("Error: No se encontró ese curso.")
        press_enter_to_continue()
        return
    course_uuid = curso_mongo.get("course_uuid")

    enroll_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    # 1. Mongo
    try:
        mongo_doc = {
            "_id": ObjectId(),
            "user_email": email,
            "course_title": course_title,
            "enroll_date": enroll_date
        }
        mongo.enrollments.insert_one(mongo_doc)
    except Exception as e:
        if "E11000" in str(e):
            print(f"\nYa estás o has estado inscrito en '{course_title}'.")
        else:
            print(f"Error fatal en Mongo: {e}")
        press_enter_to_continue()
        return

    try:
        q1 = """
        INSERT INTO student_portfolio (email, status, course_title, grade, course_id, user_id, name)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cass.execute(q1, (email, 'active', course_title, 0.0, uuid.UUID(course_uuid), uuid.UUID(user_uuid), user['name']))

        q2 = """
        INSERT INTO course_activity (course_title, status, grade, email, name, course_id, user_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cass.execute(q2, (course_title, 'active', 0.0, email, user['name'], uuid.UUID(course_uuid), uuid.UUID(user_uuid)))

    except Exception as e:
        print(f"ADVERTENCIA: Falló inscripción en Cassandra: {e}")
    
    user_uid_dgraph = dgraph_get_uid_by_email(email)
    course_uid_dgraph = dgraph_get_uid_by_title(course_title)

    if user_uid_dgraph and course_uid_dgraph:
        enroll_uid = dgraph_insert_enrollment(user_uid_dgraph, course_uid_dgraph, enroll_date)
        if not enroll_uid:
            print(f"\nFALLO: No se pudo completar inscripción en Dgraph.")
    else:
        print("\nADVERTENCIA: Usuario o curso no encontrado en Dgraph.")
        
    print(f"\nTe has inscrito al curso '{course_title}' correctamente.")
    press_enter_to_continue()

def escribir_reseña(user, mongo):
    email = user["email"]
    print("\n" + "="*80 + "\n" + "REGISTRO DE RESEÑAS".center(80) + "\n" + "="*80)
    
    cursos = list(mongo.courses.find())
    table_cursos = [[c["title"].strip()] for c in cursos]
    print(tabulate(table_cursos, headers=["Cursos disponibles"], tablefmt="fancy_grid", showindex=False))

    course_title = input("\nIngresa el nombre del curso: ").strip()
    if not course_title: return

    was_enrolled = mongo.enrollments.find_one({"user_email": email, "course_title": course_title})
    if not was_enrolled:
        print(f"\nNo puedes escribir una reseña de un curso al que no estás inscrito.")
        press_enter_to_continue()
        return

    comment = input("\nEscribe tu comentario: ").strip()
    if not comment: return

    try:
        rating = float(input("\nRating (1-10): ").strip())
        if rating < 1 or rating > 10: raise ValueError
    except Exception:
        print("\nRating inválido.")
        press_enter_to_continue()
        return

    try:
        review_doc = {
            "_id": ObjectId(),
            "course_title": course_title,
            "username": user['name'],
            "comment": comment,
            "rating": rating
        }
        mongo.reviews.insert_one(review_doc)
    except Exception as e:
        print(f"Error en Mongo: {e}")
        press_enter_to_continue()
        return

    user_uid_dgraph = dgraph_get_uid_by_email(email)
    course_uid_dgraph = dgraph_get_uid_by_title(course_title)
    if user_uid_dgraph and course_uid_dgraph:
        dgraph_insert_review(comment, rating, user_uid_dgraph, course_uid_dgraph)

    print(f"\nReseña registrada correctamente.")
    press_enter_to_continue()

def ver_mis_reseñas(user, mongo):
    print("\n" + "="*80 + "\n" + "MIS RESEÑAS".center(80) + "\n" + "="*80)
    reviews = list(mongo.reviews.find({"username": user['name']}))
    if not reviews:
        print("\nNo hay reseñas para mostrar.")
    else:
        df = pd.DataFrame(reviews)
        df = df[["course_title", "comment", "rating"]]
        print("\n" + tabulate(df, headers=["Curso", "Comentario", "Calificación"], tablefmt="fancy_grid", showindex=False))
    press_enter_to_continue()


#################################################################
# SECCIÓN 6: FUNCIONES DE INSTRUCTOR
#################################################################

def cursos_instructor(user, mongo):
    cursos = list(mongo.courses.find({"instructor_email": user['email']}))
    print("\n" + "="*80 + "\n" + "CURSOS QUE IMPARTO".center(80) + "\n" + "="*80)
    if not cursos:
        print("\nNo estás impartiendo ningún curso")
    else:
        table_data = [[c["title"].strip(), c.get("category", "").strip()] for c in cursos]
        print(tabulate(table_data, headers=["Curso", "Categoría"], tablefmt="fancy_grid"))
    press_enter_to_continue()

def instructor_anadir_leccion(user, mongo):
    print("\n" + "="*80 + "\n" + "AÑADIR LECCIÓN A CURSO".center(80) + "\n" + "="*80)
    
    mis_cursos = list(mongo.courses.find({"instructor_email": user['email']}))
    table_data = [[c['title'].strip()] for c in mis_cursos]
    print_helper_table(table_data, ["Tus Cursos"])

    course_title = input("Nombre del curso: ").strip()
    if course_title not in [c['title'] for c in mis_cursos]:
         print(f"Error: No impartes el curso '{course_title}'.")
         press_enter_to_continue()
         return

    title = input("Título de la lección: ").strip()
    description = input("Descripción: ").strip()
    url = input("URL: ").strip()

    if not title or not url:
        print("Error: Título y URL obligatorios.")
        press_enter_to_continue()
        return

    try:
        mongo.lessons.insert_one({
            "title": title, "course_title": course_title, 
            "description": description, "url": url
        })
        print(f"\nLección añadida correctamente.")
    except Exception as e:
        print(f"Error Mongo: {e}")
    press_enter_to_continue()

def calificaciones_curso(user, mongo, cass):
    print("\n" + "="*80 + "\n" + "CALIFICACIONES DEL CURSO".center(80) + "\n" + "="*80)
    
    cursos = list(mongo.courses.find({"instructor_email": user['email']}))
    if not cursos:
        print("No impartes cursos.")
        press_enter_to_continue()
        return
        
    table_cursos = [[c["title"].strip()] for c in cursos]
    print(tabulate(table_cursos, headers=["Mis Cursos"], tablefmt="fancy_grid", showindex=False))

    course_title = input("\nIngresa el nombre del curso: ").strip()
    if course_title not in [c['title'] for c in cursos]:
        print("Error: Curso no válido.")
        press_enter_to_continue()
        return

    query = "SELECT name, email, grade FROM course_activity WHERE course_title=%s AND status='completed'"
    rows = list(cass.execute(query, (course_title,)))

    if not rows:
        print(f"\nNo hay calificaciones registradas.")
    else:
        table_data = [[r.name, r.email, r.grade] for r in rows]
        print(tabulate(table_data, headers=["Alumno", "Email", "Calificación"], tablefmt="fancy_grid", showindex=False))
    press_enter_to_continue()

def alumnos_curso(user, mongo, cass):
    print("\n" + "="*80 + "\n" + "ALUMNOS ACTIVOS".center(80) + "\n" + "="*80)
    
    cursos = list(mongo.courses.find({"instructor_email": user['email']}))
    if not cursos:
        print("No impartes cursos.")
        press_enter_to_continue()
        return

    table_cursos = [[c["title"].strip()] for c in cursos]
    print(tabulate(table_cursos, headers=["Mis Cursos"], tablefmt="fancy_grid", showindex=False))

    course_title = input("\nIngresa el nombre del curso: ").strip()
    if course_title not in [c['title'] for c in cursos]: return

    query = "SELECT name, email FROM course_activity WHERE course_title=%s AND status='active'"
    rows = list(cass.execute(query, (course_title,)))

    if not rows:
        print(f"\nNo hay alumnos activos.")
    else:
        table_data = [[r.name, r.email] for r in rows]
        print(tabulate(table_data, headers=["Alumno", "Email"], tablefmt="fancy_grid", showindex=False))
    press_enter_to_continue()

def contar_lecciones_curso(mongo):
    print("\n" + "="*80 + "\n" + "CONTEO DE LECCIONES".center(80) + "\n" + "="*80)
    course_title = input("\nIngresa el nombre del curso: ").strip()
    if not course_title: return

    pipeline = [{"$match": {"course_title": course_title}}, {"$count": "total"}]
    result = list(mongo.lessons.aggregate(pipeline))
    
    if result:
        print(f"\nTotal de lecciones: {result[0]['total']}")
    else:
        print(f"\nNo se encontraron lecciones.")
    press_enter_to_continue()


#################################################################
# SECCIÓN 7: FUNCIONES DE ADMIN
#################################################################

def admin_registrar_usuario(mongo):
    print("\n" + "="*80 + "\n" + "REGISTRAR USUARIO".center(80) + "\n" + "="*80)
    name = input("Nombre: ").strip()
    email = input("Email: ").strip()
    password = input("Contraseña: ").strip()
    role = input("Rol (student/instructor): ").strip().lower()
    
    if not all([name, email, password, role]): return
    
    user_uuid = str(uuid.uuid4())
    hashed_pass = hash_password(password)
    
    try:
        mongo.users.insert_one({
            "user_uuid": user_uuid, "name": name, "email": email, 
            "password": hashed_pass, "role": role
        })
        print(f"Usuario '{name}' creado en MongoDB.")
    except Exception as e:
        print(f"Error Mongo: {e}")
        press_enter_to_continue()
        return

    dgraph_type = "User" if role == "student" else "Instructor"
    mutation = f"""
    {{ set {{ _:u <dgraph.type> "{dgraph_type}" . _:u <name> "{name}" . _:u <email> "{email}" . _:u <role> "{role}" . }} }}
    """
    dgraph_run_mutate(mutation)
    print("Usuario creado en Dgraph.")
    press_enter_to_continue()

def admin_crear_curso(mongo):
    print("\n" + "="*80 + "\n" + "CREAR CURSO".center(80) + "\n" + "="*80)
    title = input("Título: ").strip()
    category = input("Categoría: ").strip()

    instructores = list(mongo.users.find({"role": "instructor"}))
    print_helper_table([[i['name'], i['email']] for i in instructores], ["Nombre", "Email"])

    instructor_email = input("Email del instructor: ").strip()
    if not all([title, category, instructor_email]): return

    course_uuid = str(uuid.uuid4())
    try:
        mongo.courses.insert_one({
            "course_uuid": course_uuid, "title": title, 
            "category": category, "instructor_email": instructor_email
        })
        print("Curso creado en MongoDB.")
    except Exception as e:
        print(f"Error Mongo: {e}")
        press_enter_to_continue()
        return

    inst_uid = dgraph_get_uid_by_email(instructor_email)
    if inst_uid:
        mut = f"""{{ set {{ _:c <dgraph.type> "Course" . _:c <title> "{title}" . _:c <category> "{category}" . <{inst_uid}> <teaches> _:c . }} }}"""
        dgraph_run_mutate(mut)
        print("Curso creado en Dgraph.")
    else:
        print("Advertencia: Instructor no encontrado en Dgraph.")
    press_enter_to_continue()

def admin_anadir_leccion(mongo):
    print("\n" + "="*80 + "\n" + "AÑADIR LECCIÓN".center(80) + "\n" + "="*80)
    
    cursos = list(mongo.courses.find())
    print_helper_table([[c['title'], c['instructor_email']] for c in cursos], ["Curso", "Instructor"])

    course_title = input("Nombre del curso: ").strip()
    if not mongo.courses.find_one({"title": course_title}):
        print("Curso no encontrado.")
        press_enter_to_continue()
        return

    title = input("Título: ").strip()
    desc = input("Descripción: ").strip()
    url = input("URL: ").strip()

    try:
        mongo.lessons.insert_one({"title": title, "course_title": course_title, "description": desc, "url": url})
        print("Lección añadida.")
    except Exception as e:
        print(f"Error: {e}")
    press_enter_to_continue()

def admin_buscar_usuarios_por_rol(mongo):
    print("\n" + "="*80 + "\n" + "BUSCAR USUARIOS".center(80) + "\n" + "="*80)
    role = input("Rol (student/instructor/admin) o enter para todos: ").strip().lower()
    query = {"role": role} if role else {}
    users = list(mongo.users.find(query))
    if users:
        print(tabulate([[u['name'], u['email'], u['role']] for u in users], headers=["Nombre", "Email", "Rol"], tablefmt="fancy_grid"))
    else:
        print("No se encontraron usuarios.")
    press_enter_to_continue()

def admin_buscar_leccion(mongo):
    print("\n" + "="*80 + "\n" + "BUSCAR LECCIÓN".center(80) + "\n" + "="*80)
    term = input("Título o URL: ").strip()
    if not term: return
    
    lessons = list(mongo.lessons.find({"$or": [{"title": {"$regex": term, "$options": "i"}}, {"url": {"$regex": term, "$options": "i"}}]}))
    if lessons:
        print(tabulate([[l['course_title'], l['title'], l['url']] for l in lessons], headers=["Curso", "Lección", "URL"], tablefmt="fancy_grid"))
    else:
        print("No se encontraron lecciones.")
    press_enter_to_continue()

def admin_ver_reseñas_por_curso(mongo):
    print("\n" + "="*80 + "\n" + "RESEÑAS POR CURSO".center(80) + "\n" + "="*80)
    
    cursos = list(mongo.courses.find())
    print_helper_table([[c['title']] for c in cursos], ["Cursos"])

    course_title = input("Nombre del curso: ").strip()
    reviews = list(mongo.reviews.find({"course_title": course_title}))
    if reviews:
        print(tabulate([[r['username'], r['rating'], r['comment']] for r in reviews], headers=["Usuario", "Rating", "Comentario"], tablefmt="fancy_grid"))
    else:
        print("Sin reseñas.")
    press_enter_to_continue()

def consultar_logs_todos(cass):
    print("\n" + "="*80 + "\n" + "LOGS FILTRADOS POR ROL (Ordenados por Email)".center(80) + "\n" + "="*80)
    
    role = input("Ingrese rol a consultar (student/instructor/admin): ").strip().lower()
    if not role: 
        print("Debe ingresar un rol.")
        press_enter_to_continue()
        return

    query = "SELECT email, name, action, action_date FROM logs_by_role WHERE role=%s"
    
    try:
        rows = list(cass.execute(query, (role,)))
        
        if rows:
            print(f"\nResultados para rol: {role} (Ordenados A-Z)")
            print(tabulate([[r.email, r.name, r.action, r.action_date] for r in rows], 
                           headers=["Email", "Nombre", "Acción", "Fecha"], 
                           tablefmt="fancy_grid"))
        else:
            print(f"\nNo se encontraron logs para el rol '{role}'.")
            
    except Exception as e:
        print(f"Error Cassandra: {e}")

    press_enter_to_continue()
    
def consultar_logs_usuario(cass):
    print("\n" + "="*80 + "\n" + "LOGS POR USUARIO".center(80) + "\n" + "="*80)
    
    email = input("Email a consultar: ").strip()
    if not email: return
    
    query = "SELECT email, action, action_date FROM logs_by_user WHERE email=%s"
    rows = list(cass.execute(query, (email,)))
    if rows:
        print(tabulate([[r.email, r.action, r.action_date] for r in rows], headers=["Email", "Acción", "Fecha"], tablefmt="fancy_grid"))
    else:
        print("Sin logs para este usuario.")
    press_enter_to_continue()

def consultar_calificaciones(cass, mongo):
    print("\n" + "="*80 + "\n" + "CALIFICACIONES HISTÓRICAS".center(80) + "\n" + "="*80)
    
    cursos = list(mongo.courses.find())
    print_helper_table([[c['title']] for c in cursos], ["Cursos"])

    course_title = input("Nombre del curso: ").strip()
    query = "SELECT name, email, grade FROM course_activity WHERE course_title=%s AND status='completed'"
    rows = list(cass.execute(query, (course_title,)))
    if rows:
        print(tabulate([[r.name, r.email, r.grade] for r in rows], headers=["Alumno", "Email", "Nota"], tablefmt="fancy_grid"))
    else:
        print("Sin calificaciones.")
    press_enter_to_continue()

def alumnos_reprobados(cass, mongo):
    print("\n" + "="*80 + "\n" + "ALUMNOS REPROBADOS".center(80) + "\n" + "="*80)
    
    cursos = list(mongo.courses.find())
    print_helper_table([[c['title']] for c in cursos], ["Cursos"])

    course_title = input("Nombre del curso: ").strip()
    query = "SELECT name, email, grade FROM course_activity WHERE course_title=%s AND status='completed' AND grade < 6"
    rows = list(cass.execute(query, (course_title,)))
    
    if rows:
        print(tabulate([[r.name, r.email, r.grade] for r in rows], headers=["Alumno", "Email", "Nota"], tablefmt="fancy_grid"))
    else:
        print("No hay reprobados.")
    press_enter_to_continue()

def contar_alumnos(cass, mongo):
    print("\n" + "="*80 + "\n" + "CONTAR ALUMNOS ACTIVOS".center(80) + "\n" + "="*80)
    
    cursos = list(mongo.courses.find())
    print_helper_table([[c['title']] for c in cursos], ["Cursos"])

    course_title = input("Nombre del curso: ").strip()
    query = "SELECT COUNT(*) FROM course_activity WHERE course_title=%s AND status='active'"
    row = cass.execute(query, (course_title,)).one()
    print(f"Alumnos activos: {row[0]}")
    press_enter_to_continue()

def probar_conexiones(mongo, cass):
    """ Realiza una prueba de conexión a las 3 bases de datos. """
    print("\n" + "="*80)
    print("PRUEBA DE CONEXIONES DE LAS BASES DE DATOS".center(80))
    print("="*80)
    
    try:
        mongo.client.server_info() 
        print(f"\nMongoDB: OK (Conectado a '{mongo.name}')")
    except Exception as e: print(f"\nMongoDB: ERROR ({e})")

    try:
        cass.execute("SELECT cluster_name FROM system.local")
        print(f"Cassandra: OK (Keyspace: '{cass.keyspace}')")
    except Exception as e: print(f"Cassandra: ERROR ({e})")

    try:
        res = requests.get("http://127.0.0.1:8080/health")
        res.raise_for_status()
        health_data = res.json()
        if isinstance(health_data, list) and len(health_data) > 0:
            print(f"Dgraph: OK (Versión: {health_data[0].get('version', 'desconocida')})")
        else:
            print(f"Dgraph: OK (Respuesta de /health recibida, pero formato inesperado: {health_data})")
    except requests.exceptions.RequestException as e:
        print(f"Dgraph: ERROR (No se pudo conectar: {e})")
    except Exception as e: 
        print(f"Dgraph: ERROR (Respuesta inesperada: {e})")

    press_enter_to_continue()


#################################################################
# SECCIÓN 8: SUB-MENÚ DE REPORTES DGRAPH (Admin)
#################################################################

def menu_reportes_dgraph(user):
    while True:
        menu_items = [
            ["1", "Instructor y sus Alumnos (D1)"],
            ["2", "Popularidad de Cursos (D2)"],
            ["3", "Colaboración de Instructores (D3)"],
            ["4", "Recomendar Cursos (D4)"],
            ["5", "Influencia de Instructores (D5)"],
            ["6", "Análisis Conexiones Cruzadas (D6)"],
            ["7", "Afinidad de Alumno (D7)"],
            ["8", "Conexiones Indirectas (D8)"],
            ["9", "Recomendaciones de Red (D9)"],
            ["10", "Análisis de Reseñas (D10)"],
            ["11", "Historial Alumno-Instructor (D11)"],
            ["12", "Desempeño Promedio por Categoría (D12)"],
            ["13", "Regresar"]
        ]
        print(f"\n===== Reportes Dgraph =====\n")
        print(tabulate(menu_items, tablefmt="fancy_grid"))
        choice = input("\nOpción: ")
        clear_screen()

        if choice == "1": dgraph_report_D1()
        elif choice == "2": dgraph_report_D2()
        elif choice == "3": dgraph_report_D3()
        elif choice == "4": dgraph_report_D4(user, is_student_mode=False) 
        elif choice == "5": dgraph_report_D5()
        elif choice == "6": dgraph_report_D6()
        elif choice == "7": dgraph_report_D7(user, is_student_mode=False)
        elif choice == "8": dgraph_report_D8(user, is_student_mode=False)
        elif choice == "9": dgraph_report_D9()
        elif choice == "10": dgraph_report_D10()
        elif choice == "11": dgraph_report_D11(user, is_student_mode=False)
        elif choice == "12": dgraph_report_D12()
        elif choice == "13": return
        else: print("\nOpción no válida")
        if choice in [str(i) for i in range(1, 13)]: press_enter_to_continue()


def dgraph_report_D1():
    print("--- (D1) Instructor y sus Alumnos ---")
    
    q = "{ i(func: type(Instructor)) { name email } }"
    res = dgraph_run_query(q)
    if res: print_helper_table([[x.get('name'), x.get('email')] for x in res.get('data', {}).get('i', [])], ["Nombre", "Email"])

    email = input("Email instructor: ").strip()
    if not email: return
    
    q = f"""{{
      inst(func: eq(email, "{email}")) @filter(type(Instructor)) {{
        name
        teaches {{
          title
          ~of_course {{
            ~enrolled_in {{ name email }}
          }}
        }}
      }}
    }}"""
    data = dgraph_run_query(q)
    if not data or not data['data']['inst']:
        print("Instructor no encontrado.")
        return
    
    inst = data['data']['inst'][0]
    print(f"\nInstructor: {inst['name']}")
    for cur in inst.get('teaches', []):
        print(f"  Curso: {cur['title']}")
        enrolls = cur.get('~of_course', [])
        for e in enrolls:
            for s in e.get('~enrolled_in', []):
                print(f"    - {s['name']} ({s['email']})")

def dgraph_report_D2():
    print("--- (D2) Popularidad de Cursos ---")
    q = """{
      c(func: type(Course)) {
        title
        count(~of_course)
        count(~review_of)
      }
    }"""
    data = dgraph_run_query(q)
    if data:
        rows = [[c['title'], c.get('count(~of_course)'), c.get('count(~review_of)')] for c in data['data']['c']]
        print(tabulate(rows, headers=["Curso", "Inscripciones", "Reseñas"], tablefmt="fancy_grid"))

def dgraph_report_D3():
    print("--- (D3) Colaboración Instructores ---")
    q = """{
      i(func: type(Instructor)) {
        name
        teaches { 
          category
          ~of_course { ~enrolled_in { uid } } 
        }
      }
    }"""
    data = dgraph_run_query(q)
    insts = data['data']['i']
    
    inst_data = {}
    for i in insts:
        students = set()
        categories = set()
        for c in i.get('teaches', []):
            if 'category' in c: categories.add(c['category'])
            for e in c.get('~of_course', []):
                for stu in e.get('~enrolled_in', []): students.add(stu['uid'])
        inst_data[i['name']] = {'students': students, 'categories': categories}
        
    pairs = []
    names = list(inst_data.keys())
    for idx, n1 in enumerate(names):
        for n2 in names[idx+1:]:
            d1 = inst_data[n1]
            d2 = inst_data[n2]
            common_students = len(d1['students'].intersection(d2['students']))
            common_cats = list(d1['categories'].intersection(d2['categories']))
            if common_students > 0 or common_cats:
                reason = []
                if common_students: reason.append(f"{common_students} Alumnos")
                if common_cats: reason.append(f"Cat: {', '.join(common_cats)}")
                pairs.append([n1, n2, " + ".join(reason)])
            
    if pairs: print(tabulate(pairs, headers=["Inst A", "Inst B", "Motivo Relación"], tablefmt="fancy_grid"))
    else: print("Sin colaboraciones encontradas.")

def dgraph_report_D4(user, is_student_mode=False):
    print("--- (D4) Recomendar Cursos (Por Categoría o Instructor) ---")
    email = user['email'] if is_student_mode else ""
    if not is_student_mode:
        q = "{ s(func: type(User)) { name email } }"
        res = dgraph_run_query(q)
        if res: print_helper_table([[x.get('name'), x.get('email')] for x in res['data']['s']], ["Estudiante", "Email"])
        while not email: email = input("Email estudiante: ").strip()

    uid = dgraph_get_uid_by_email(email)
    if not uid: 
        print("Usuario no encontrado.")
        return

    q = f"""{{
      u(func: uid({uid})) {{
        enrolled_in {{
          of_course {{
            uid
            category
            ~teaches {{ uid }}  
          }}
        }}
      }}
    }}"""
    data = dgraph_run_query(q)
    taken_uids = set()
    fav_cats = set()
    fav_inst_uids = set()
    
    for e in data['data']['u'][0].get('enrolled_in', []):
        c = e.get('of_course')
        if c: 
            taken_uids.add(c['uid'])
            if 'category' in c: fav_cats.add(c['category'])
            for inst in c.get('~teaches', []):
                fav_inst_uids.add(inst['uid'])
            
    if not fav_cats and not fav_inst_uids:
        print("El usuario no ha tomado cursos suficientes para recomendar.")
        return

    cat_block = " OR ".join([f'eq(category, "{c}")' for c in fav_cats]) if fav_cats else "eq(val(0), 1)"
    inst_uids_str = ", ".join(fav_inst_uids)
    
    q_rec = f"""{{
      by_cat(func: type(Course)) @filter({cat_block} AND NOT uid({", ".join(taken_uids)})) {{
        title
        category
      }}
      by_inst(func: uid({inst_uids_str})) {{
        teaches @filter(NOT uid({", ".join(taken_uids)})) {{
          title
          category
        }}
      }}
    }}"""
    res = dgraph_run_query(q_rec)
    recommendations = []
    
    for c in res['data'].get('by_cat', []):
        recommendations.append([c['title'], c['category'], "Misma Categoría"])
    for i in res['data'].get('by_inst', []):
        for c in i.get('teaches', []):
            if not any(r[0] == c['title'] for r in recommendations):
                recommendations.append([c['title'], c['category'], "Mismo Instructor"])

    if recommendations:
        print(tabulate(recommendations, headers=["Curso Recomendado", "Categoría", "Razón"], tablefmt="fancy_grid"))
    else:
        print("No hay recomendaciones nuevas.")

def dgraph_report_D5():
    print("--- (D5) Influencia Instructores ---")
    q = """{
      i(func: type(Instructor)) {
        name
        teaches {
          count(~of_course)
        }
      }
    }"""
    data = dgraph_run_query(q)
    
    if not data or 'data' not in data:
        print("No se recibieron datos de Dgraph.")
        return

    rows = []
    for i in data['data']['i']:
        cursos = i.get('teaches', [])
        total_alumnos = sum([c.get('count(~of_course)', 0) for c in cursos])
        
        rows.append([i['name'], total_alumnos])
        
    rows.sort(key=lambda x: x[1], reverse=True)
    print(tabulate(rows, headers=["Instructor", "Total Alumnos"], tablefmt="fancy_grid"))
    
def dgraph_report_D6():
    print("--- (D6) Conexiones Cruzadas ---")
    q = """{
      u(func: type(User)) {
        name
        enrolled_in {
          of_course {
            category
            ~teaches { name }
          }
        }
      }
    }"""
    data = dgraph_run_query(q)
    if not data or 'data' not in data or not data['data']['u']:
        print("No se encontraron datos.")
        return

    rows = []
    for u in data['data']['u']:
        user_name = u.get('name', 'Desconocido')
        cat_counts = {}
        inst_counts = {}
        enrollments = u.get('enrolled_in', [])
        if not enrollments: continue

        for e in enrollments:
            course = e.get('of_course', {})
            cat = course.get('category')
            if cat: cat_counts[cat] = cat_counts.get(cat, 0) + 1
            for inst in course.get('~teaches', []):
                inst_name = inst.get('name')
                if inst_name: inst_counts[inst_name] = inst_counts.get(inst_name, 0) + 1

        for cat, count in cat_counts.items():
            if count > 1: rows.append([user_name, "Categoría", cat, count])
        for inst, count in inst_counts.items():
            if count > 1: rows.append([user_name, "Instructor", inst, count])

    if rows:
        rows.sort(key=lambda x: x[0])
        print(tabulate(rows, headers=["Alumno", "Tipo Conexión", "Valor", "Cursos"], tablefmt="fancy_grid"))
    else:
        print("Sin conexiones cruzadas.")

def dgraph_report_D7(user, is_student_mode=False):
    print("--- (D7) Afinidad ---")
    email = user['email'] if is_student_mode else ""
    if not is_student_mode:
        q = "{ s(func: type(User)) { name email } }"
        res = dgraph_run_query(q)
        if res: print_helper_table([[x.get('name'), x.get('email')] for x in res['data']['s']], ["Estudiante", "Email"])
        while not email: email = input("Email estudiante: ").strip()

    uid = dgraph_get_uid_by_email(email)
    if not uid: return

    q = f"""{{
      u(func: uid({uid})) {{ enrolled_in {{ of_course {{ category }} }} }}
    }}"""
    data = dgraph_run_query(q)
    cats = {}
    for e in data['data']['u'][0].get('enrolled_in', []):
        c = e.get('of_course', {}).get('category')
        if c: cats[c] = cats.get(c, 0) + 1
    
    print(tabulate([[k, v] for k,v in cats.items()], headers=["Categoría", "Total"], tablefmt="fancy_grid"))

def dgraph_report_D8(user, is_student_mode=False):
    print("--- (D8) Conexiones Indirectas ---")
    email = user['email'] if is_student_mode else ""
    if not is_student_mode:
        q = "{ s(func: type(User)) { name email } }"
        res = dgraph_run_query(q)
        if res: print_helper_table([[x.get('name'), x.get('email')] for x in res['data']['s']], ["Estudiante", "Email"])
        while not email: email = input("Email estudiante: ").strip()

    uid = dgraph_get_uid_by_email(email)
    if not uid: return

    q = f"""{{
      u(func: uid({uid})) {{
        enrolled_in {{
          of_course {{
            ~teaches {{
              teaches {{
                ~of_course {{
                  ~enrolled_in @filter(not uid({uid})) {{ name email }}
                }}
              }}
            }}
          }}
        }}
      }}
    }}"""
    data = dgraph_run_query(q)
    peers = set()
    for e in data['data']['u'][0].get('enrolled_in', []):
        for i in e.get('of_course', {}).get('~teaches', []):
            for c in i.get('teaches', []):
                for e2 in c.get('~of_course', []):
                    for u in e2.get('~enrolled_in', []):
                        peers.add(f"{u['name']} ({u.get('email', '')})")
    
    if peers: print(tabulate([[p] for p in peers], headers=["Compañeros de Red"], tablefmt="fancy_grid"))
    else: print("Sin conexiones indirectas.")

def dgraph_report_D9():
    print("--- (D9) Recomendaciones de Red ---")
    print("(Estudiantes con 2+ cursos en común)")
    q = """{
      u(func: type(User)) { name enrolled_in { of_course { uid } } }
    }"""
    data = dgraph_run_query(q)
    users = data['data']['u']
    user_courses = {u['name']: set([e['of_course']['uid'] for e in u.get('enrolled_in', []) if 'of_course' in e]) for u in users}
    
    names = list(user_courses.keys())
    rows = []
    for idx, n1 in enumerate(names):
        for n2 in names[idx+1:]:
            common = len(user_courses[n1].intersection(user_courses[n2]))
            if common >= 2: rows.append([n1, n2, common])
            
    if rows: print(tabulate(rows, headers=["User A", "User B", "Cursos Común"], tablefmt="fancy_grid"))
    else: print("Nadie comparte 2 o más cursos.")

def dgraph_report_D10():
    print("--- (D10) Análisis de Reseñas  ---")
    
    q_courses = """{
      c(func: type(Course)) {
        title
        ~review_of { rating }
      }
    }"""
    data_c = dgraph_run_query(q_courses)
    rows_c = []
    for c in data_c['data']['c']:
        ratings = [float(r['rating']) for r in c.get('~review_of', [])]
        avg = sum(ratings)/len(ratings) if ratings else 0
        rows_c.append([c['title'], f"{avg:.2f}", len(ratings)])
    
    print("\n>>> Desempeño por CURSO")
    print(tabulate(rows_c, headers=["Curso", "Promedio", "Total Reseñas"], tablefmt="fancy_grid"))

    q_inst = """{
      i(func: type(Instructor)) {
        name
        teaches {
          ~review_of { rating }
        }
      }
    }"""
    data_i = dgraph_run_query(q_inst)
    rows_i = []
    for i in data_i['data']['i']:
        all_ratings = []
        for c in i.get('teaches', []):
            all_ratings.extend([float(r['rating']) for r in c.get('~review_of', [])])
            
        avg = sum(all_ratings)/len(all_ratings) if all_ratings else 0
        rows_i.append([i['name'], f"{avg:.2f}", len(all_ratings)])
        
    print("\n>>> Desempeño por INSTRUCTOR")
    print(tabulate(rows_i, headers=["Instructor", "Promedio General", "Total Reseñas"], tablefmt="fancy_grid"))

def dgraph_report_D11(user, is_student_mode=False):
    print("--- (D11) Historial Alumno-Instructor ---")
    email = user['email'] if is_student_mode else ""
    if not is_student_mode:
        q = "{ s(func: type(User)) { name email } }"
        res = dgraph_run_query(q)
        if res: print_helper_table([[x.get('name'), x.get('email')] for x in res['data']['s']], ["Estudiante", "Email"])
        while not email: email = input("Email estudiante: ").strip()

    uid = dgraph_get_uid_by_email(email)
    if not uid: return

    q = f"""{{
      u(func: uid({uid})) {{
        enrolled_in {{
          of_course {{
            title
            ~teaches {{ name }}
          }}
        }}
      }}
    }}"""
    data = dgraph_run_query(q)
    hist = []
    for e in data['data']['u'][0].get('enrolled_in', []):
        c = e.get('of_course', {})
        insts = [i['name'] for i in c.get('~teaches', [])]
        hist.append([c.get('title'), ", ".join(insts)])
    print(tabulate(hist, headers=["Curso Tomado", "Instructor(es)"], tablefmt="fancy_grid"))

def dgraph_report_D12():
    print("--- (D12) Desempeño por Categoría ---")
    q = """{
      c(func: type(Course)) {
        category
        ~review_of { rating }
      }
    }"""
    data = dgraph_run_query(q)
    cats = {}
    for c in data['data']['c']:
        cat = c.get('category')
        ratings = [float(r['rating']) for r in c.get('~review_of', [])]
        if cat and ratings:
            if cat not in cats: cats[cat] = []
            cats[cat].extend(ratings)
            
    rows = []
    for k, v in cats.items():
        rows.append([k, f"{sum(v)/len(v):.2f}"])
    print(tabulate(rows, headers=["Categoría", "Rating Promedio"], tablefmt="fancy_grid"))


#################################################################
# SECCIÓN 9: PUNTO DE ARRANQUE
#################################################################

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    
    print("Iniciando conexiones...")
    try:
        mongo_conn = connect_mongo()
        cass_conn = connect_cassandra()
        print("Conexiones exitosas.")
    except Exception as e:
        print(f"\nError fatal: {e}")
        sys.exit(1)

    user = None
    while not user:
        try:
            user = login(mongo_conn, cass_conn)
            if user is None: time.sleep(1)
        except KeyboardInterrupt:
            sys.exit()
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(1)
                
    main_menu(user, mongo_conn, cass_conn)