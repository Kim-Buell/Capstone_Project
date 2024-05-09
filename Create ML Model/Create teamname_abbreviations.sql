CREATE TABLE teamname_abbreviations (
    Abbreviation VARCHAR(3),
    "Full Name" VARCHAR(255)
);

\copy teamname_abbreviations FROM 'C:\Users\KLBue\OneDrive\Documents\MS Data Science\Capstone Project\Git Hub Submission\Create ML Model\teamname_abbreviations.csv' DELIMITER ',' CSV HEADER;
