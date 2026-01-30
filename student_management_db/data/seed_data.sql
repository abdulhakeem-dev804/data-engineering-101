-- Sample data for Student Management System

-- 1. Departments (no dependencies)
INSERT INTO departments (name, building) VALUES
('Computer Science', 'Tech Building'),
('Mathematics', 'Science Hall'),
('Physics', 'Science Hall'),
('English', 'Arts Building');

-- 2. Instructors (depends on departments)
INSERT INTO instructors (first_name, last_name, email, date_of_birth, salary, department_id) VALUES
('Arun', 'Sharma', 'arun.sharma@university.edu', '1975-03-15', 85000, 1),
('Priya', 'Iyer', 'priya.iyer@university.edu', '1980-07-22', 78000, 2),
('Rajesh', 'Kumar', 'rajesh.kumar@university.edu', '1978-11-08', 82000, 3),
('Meera', 'Nair', 'meera.nair@university.edu', '1982-04-30', 75000, 4);

-- 3. Students (depends on departments)
INSERT INTO students (first_name, last_name, email, date_of_birth, department_id) VALUES
('Rahul', 'Verma', 'rahul.verma@student.edu', '2002-05-15', 1),
('Priya', 'Patel', 'priya.patel@student.edu', '2001-08-22', 1),
('Amit', 'Singh', 'amit.singh@student.edu', '2002-01-10', 2),
('Sneha', 'Reddy', 'sneha.reddy@student.edu', '2001-11-30', 3),
('Vikram', 'Joshi', 'vikram.joshi@student.edu', '2002-03-18', 2);

-- 4. Courses (depends on departments)
INSERT INTO courses (course_code, name, credits, department_id) VALUES
('CS101', 'Introduction to Programming', 4, 1),
('CS201', 'Data Structures', 4, 1),
('MATH101', 'Calculus I', 3, 2),
('PHY101', 'Physics Fundamentals', 4, 3),
('ENG101', 'English Composition', 3, 4);

-- 5. Classes (depends on courses, instructors)
INSERT INTO classes (course_id, instructor_id, semester, year, schedule) VALUES
(1, 1, 'Spring', 2024, 'Mon/Wed 10:00-11:30'),
(2, 1, 'Spring', 2024, 'Tue/Thu 14:00-15:30'),
(3, 2, 'Spring', 2024, 'Mon/Wed/Fri 09:00-10:00'),
(4, 3, 'Spring', 2024, 'Tue/Thu 10:00-11:30'),
(5, 4, 'Spring', 2024, 'Mon/Wed 13:00-14:30');

-- 6. Enrollments (depends on students, classes)
INSERT INTO enrollments (student_id, class_id) VALUES
(1, 1), (1, 2), (1, 3),
(2, 1), (2, 5),
(3, 3), (3, 5),
(4, 4), (4, 3),
(5, 3), (5, 1);