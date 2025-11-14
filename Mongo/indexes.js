
//Usuarios: índice único por email
db.users.createIndex({ email: 1 }, { unique: true });

//Cursos: índice de texto para búsquedas por título y categoría
db.courses.createIndex({ title: "text", category: "text" });

//Lecciones: índice para buscar por título y curso
db.lessons.createIndex({ title: "text", course_title: 1 });

//Inscripciones: índice único para que un usuario no se inscriba dos veces en el mismo curso
db.enrollments.createIndex({ email: 1, course_title: 1 }, { unique: true });

//Reseñas: índice para filtrar rápidamente reseñas por curso y usuario
db.reviews.createIndex({ course_title: 1, username: 1 });
