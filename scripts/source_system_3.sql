CREATE DATABASE source_system_3;

CREATE TABLE CustomerVisit(
    "id" integer not null generated always as identity (increment by 1),
    "name" varchar(100) NULL,
    "gender" boolean NULL,
    "checkin_time" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    "checkout_time" TIMESTAMP WITH TIME ZONE DEFAULT NULL
);

INSERT INTO CustomerVisit (name, gender, checkin_time, checkout_time)
VALUES  ('Nguyen Trong Toan', true, '2023-10-31 15:00:00', '2023-10-31 16:00:00'),
        ('Nguyen Thi Bich Ngoc', false, '2023-11-01 15:20:00', '2023-11-01 15:30:00'),
        ('Cao Thi My Uyen', false, '2023-10-30 07:00:00', '2023-10-30 10:00:00')