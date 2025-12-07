
# PROYECTO 

Plataforma de Educación en Línea - Learnlink

MongoDB: Guarda perfiles de usuario, materiales de curso y lecciones.
Dgraph: Rastrea inscripciones y relaciones entre estudiantes e instructores.
Cassandra: Registra la actividad y el progreso de los estudiantes a lo largo del tiempo (p. ej., cuestionarios, sesiones).
  

# DESCRIPCION 


# Archivos incluidos
- `Cassandra/schema.cql`
- `data/cassandra_data.json`
- `data/dgraph_data.rdf`
- `data/mongo_data.json`
- `Dgraph/schema.dql`
- `Mongo/indexes.js`
- `connect.py`
- `docker-compose.yml`
- `main.py`
- `populate.py`
- `README.md`
- `requirements.txt`


# INTEGRANTES EQUIPO 4

Oscar Abraham Ascencio Muñoz 
Sarah Martinez Mora 


# COMANDOS PARA CORRER EL PROGRAMA

# 1. Iniciar Entorno 
docker-compose down -v
docker-compose up -d
# 2. Cargar esquema cassandra
docker cp ./Cassandra/schema.cql proyectoedtech-cassandra-1:/tmp/schema.cql
docker exec -i proyectoedtech-cassandra-1 cqlsh -f /tmp/schema.cql 
# 3. Poblar Bases de Datos 
python populate.py
# 4. Ejecutar Aplicación
python main.py

# CASOS DE USO 

# Administrador
usuario: admin
contraseña: 1234
# Instructor
usuario: carla.gomez@example.com
contraseña: 12345678
# Alumno
usuario: ana.torres@example.com
contraseña: 12345678
