DROP DATABASE IF EXISTS inf551;
CREATE DATABASE IF NOT EXISTS inf551;

use inf551;

drop table if EXISTS inspections;
drop table if EXISTS violations;


CREATE TABLE IF NOT EXISTS inspections(
    serial_number VARCHAR(10), 
    activity_date DATETIME,
    facility_name VARCHAR(100),
    score TINYINT,
    grade VARCHAR(2),
    service_code VARCHAR(10),
    service_description VARCHAR(50),
    employee_id VARCHAR(20),
    facility_address VARCHAR(100),
    facility_city VARCHAR(50),
    facility_id VARCHAR(20),
    facility_state VARCHAR(5),
    facility_zip VARCHAR(20),
    owner_id VARCHAR(20),
    pe_description VARCHAR(50),
    program_element_pe VARCHAR(50),
    program_name VARCHAR(50),
    program_status VARCHAR(10),
    record_id VARCHAR(20)
    
);

CREATE TABLE IF NOT EXISTS violations(
    serial_number VARCHAR(10), 
    activity_date DATETIME,
    facility_name VARCHAR(100),
    violation_code VARCHAR(10),
    violation_description VARCHAR(50),
    violation_status VARCHAR(20),
    points TINYINT,
    grade VARCHAR(2),
    facility_address VARCHAR(100),
    facility_city VARCHAR(50),
    facility_id VARCHAR(20),
    facility_state VARCHAR(5),
    facility_zip VARCHAR(20),
    employee_id VARCHAR(20),
    owner_id VARCHAR(20),
    owner_name VARCHAR(20),
    pe_description VARCHAR(50),
    program_element_pe VARCHAR(50),
    program_name VARCHAR(50),
    program_status VARCHAR(50),
    record_id VARCHAR(20),
    score  TINYINT,
    service_code VARCHAR(5),
    service_description VARCHAR(50),
    row_id VARCHAR(20)
    
);

LOAD DATA LOCAL 
-- INFILE "D:/551 Foundations of Data Management/HW3/Peiying_Lyu_HW3/inspections.csv" 
INFILE "inspections.csv"
INTO TABLE inspections
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;


LOAD DATA LOCAL 
-- INFILE "D:/551 Foundations of Data Management/HW3/Peiying_Lyu_HW3/violations.csv" 
INFILE "violations.csv"
INTO TABLE violations
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;