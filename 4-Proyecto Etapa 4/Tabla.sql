DROP TABLE IF EXISTS CompanyProfiles;
CREATE TABLE CompanyProfiles (
    Symbol VARCHAR(10) PRIMARY KEY,
    Company VARCHAR(255),
    Sector VARCHAR(100),
    SubIndustry VARCHAR(255)  
);

DROP TABLE IF EXISTS Companies;
CREATE TABLE Companies (
    "Date" DATE,
    Symbol VARCHAR(10),
    [Close] FLOAT,
    PRIMARY KEY ("Date", Symbol)
);
Select * from Companies;
Select * from CompanyProfiles;
