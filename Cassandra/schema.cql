-- Cassandra Schema
CREATE KEYSPACE learnlink WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE learnlink;

CREATE TABLE logging_info_by_user (
    email TEXT,
    user_id UUID,
    name TEXT,
    action TEXT,
    action_date TIMESTAMP,
    PRIMARY KEY ((email), action, action_date)

);

CREATE TABLE course_info_by_status (
    user_id UUID,
    course_id UUID,
    course_title  TEXT,
    email TEXT,
    name TEXT,
    status TEXT,
    grade FLOAT,
    PRIMARY KEY ((email), status, course_title)
);
