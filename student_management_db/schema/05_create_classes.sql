CREATE TABLE classes (
    class_id SERIAL PRIMARY KEY,
    course_id INTEGER NOT NULL REFERENCES courses(course_id),
    instructor_id INTEGER REFERENCES instructors(instructor_id),
    semester VARCHAR(20) NOT NULL,
    year INTEGER NOT NULL,
    max_students INTEGER DEFAULT 30,
    schedule VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT valid_year CHECK (year >= 2000 AND year <= 2100)
);
