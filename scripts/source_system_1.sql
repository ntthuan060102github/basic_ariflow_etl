CREATE DATABASE source_system_1;

CREATE TABLE CustomerVisit(
    "id" integer not null generated always as identity (increment by 1),
    "customer_name" varchar(100) NULL,
    "gender" varchar(1) NULL,
    "arrival_time" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    "departure_time" TIMESTAMP WITH TIME ZONE DEFAULT NULL
);

INSERT INTO CustomerVisit (customer_name, gender, arrival_time, departure_time)
VALUES  ('Nguyen Trong Thuan', 'M', '2023-10-29 15:00:00', '2023-10-29 16:00:00'),
        ('Trinh Thi Tuyet Nhung', 'F', '2023-10-29 15:20:00', '2023-10-29 15:30:00'),
        ('Vo Ngoc Son', 'M', '2023-10-30 07:00:00', '2023-10-30 10:00:00'),
        ('Huynh Quoc Thai', 'M', '2023-10-30 20:00:00', '2023-10-30 22:00:00'),
        ('Nguyen Trong Binh', 'M', '2023-10-30 08:00:00', '2023-10-30 12:00:00')