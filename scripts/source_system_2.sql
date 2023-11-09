CREATE DATABASE source_system_2;

CREATE TABLE CustomerAttendanceLog(
    "id" integer not null generated always as identity (increment by 1),
    "full_name" varchar(100) NULL,
    "gender" varchar(10) NULL,
    "checkin_time" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    "checkout_time" TIMESTAMP WITH TIME ZONE DEFAULT NULL,
    "created_at" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    "last_updated" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO CustomerAttendanceLog (full_name, gender, checkin_time, checkout_time)
VALUES  ('Nguyen Trong Tu', 'male', '2023-10-31 15:00:00', '2023-10-31 16:00:00'),
        ('Nguyen Thi Hang', 'female', '2023-11-01 15:20:00', '2023-11-01 15:30:00'),
        ('Nguyen Thi Ngoc Anh', 'female', '2023-10-30 07:00:00', '2023-10-30 10:00:00')