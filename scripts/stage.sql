create database stage;

create table CustomerAttendanceLog (
  "id" integer not null generated always as identity (increment by 1),
  "nk" integer not null,
  "source_system" integer not null,
  "full_name" varchar(100) NULL,
  "gender" boolean NULL,
  "arrival_time" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  "departure_time" TIMESTAMP WITH TIME ZONE DEFAULT NULL,
  "created_at" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
  "last_updated" TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
