drop table if exists A CASCADE;
drop table if exists B CASCADE;
drop table if exists C CASCADE;
drop table if exists D CASCADE;
drop table if exists E CASCADE;
drop table if exists F CASCADE;
drop table if exists G CASCADE;
drop table if exists H CASCADE;
drop table if exists "MixedCaseTable" CASCADE;
drop table if exists UPPER_CASE_TABLE CASCADE;

CREATE TABLE "MixedCaseTable"
  (COL1 VARCHAR(32),
   PRIMARY KEY (COL1));
CREATE TABLE UPPER_CASE_TABLE
  (COL1 VARCHAR(32),
   PRIMARY KEY (COL1));

ALTER TABLE UPPER_CASE_TABLE ADD CONSTRAINT FK1 FOREIGN KEY (COL1) REFERENCES "MixedCaseTable" (COL1);
