CREATE TABLE teamname_abbreviations (
    Abbreviation VARCHAR(3),
    "Full Name" VARCHAR(255)
);

\copy teamname_abbreviations FROM 'C:\path_name\teamname_abbreviations.csv' DELIMITER ',' CSV HEADER; # Enter Path Name
