create database metadata;

create table if not exists RDBExtractData  (
  "ID" integer not null generated always as identity (increment by 1),
  "table_name" varchar(50) NULL,
  "source_system" INT NOT NULL,
  "LSET" TIMESTAMP WITH TIME ZONE DEFAULT NULL
);

INSERT INTO RDBExtractData ("table_name", "source_system") VALUES ('CustomerVisit', 1);
INSERT INTO RDBExtractData ("table_name", "source_system") VALUES ('CustomerAttendanceLog', 2);
INSERT INTO RDBExtractData ("table_name", "source_system") VALUES ('CustomerVisit', 3);