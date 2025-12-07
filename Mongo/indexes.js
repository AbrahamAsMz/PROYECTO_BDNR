// Este archivo solo muestra los indices que se implementaron en mongo, los
// indices no se cargan desde este archivo, se cargan desde las lineas 
// agregadas en el populate.py

use learnlink; 

db.users.createIndex({ email: 1 }, { unique: true });
db.users.createIndex({ user_uuid: 1 }, { unique: true }); 

db.courses.createIndex({ title: "text", category: "text" });
db.courses.createIndex({ course_uuid: 1 }, { unique: true });

db.lessons.createIndex({ title: "text", course_title: 1 });

db.enrollments.createIndex({ user_email: 1, course_title: 1 }, { unique: true });

db.reviews.createIndex({ course_title: 1, username: 1 });
