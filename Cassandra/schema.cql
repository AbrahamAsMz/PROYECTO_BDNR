CREATE KEYSPACE IF NOT EXISTS learnlink 
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE learnlink;

CREATE TABLE IF NOT EXISTS logs_by_user (
    email TEXT,
    action TEXT,
    action_date TIMESTAMP,
    user_id UUID,
    name TEXT,
    role TEXT,
    PRIMARY KEY ((email), action, action_date)
) WITH CLUSTERING ORDER BY (action ASC, action_date DESC);

CREATE TABLE IF NOT EXISTS logs_by_role (
    role TEXT,
    email TEXT,
    action_date TIMESTAMP,
    name TEXT,
    action TEXT,
    user_id UUID,
    PRIMARY KEY ((role), email, action_date, action)
) WITH CLUSTERING ORDER BY (email ASC, action_date DESC, action ASC);

CREATE TABLE IF NOT EXISTS student_portfolio (
    email TEXT,
    status TEXT,
    course_title TEXT,
    grade FLOAT,
    course_id UUID,
    user_id UUID,
    name TEXT,
    PRIMARY KEY ((email), status, course_title)
);

CREATE TABLE IF NOT EXISTS course_activity (
    course_title TEXT,
    status TEXT,
    grade FLOAT,
    email TEXT,
    name TEXT,
    course_id UUID,
    user_id UUID,
    PRIMARY KEY ((course_title), status, grade, email)
);