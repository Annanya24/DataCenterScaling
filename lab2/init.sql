CREATE TABLE outcomes(
    animal_id VARCHAR,
    animal_name VARCHAR,
    ts TIMESTAMP,
    dob DATE,
    outcome_type VARCHAR,
    outcome_subtype VARCHAR,
    animal_type VARCHAR,
    age VARCHAR,
    breed VARCHAR,
    color VARCHAR,
    month VARCHAR,
    year INT,
    sex VARCHAR,
    sterilization VARCHAR
);


-- Animal Dimension
CREATE TABLE animal_dim (
    animal_dim_key SERIAL PRIMARY KEY,
    animal_id VARCHAR,
    animal_name VARCHAR,
    breed VARCHAR,
    color VARCHAR,
    dob DATE,
    outcome_subtype VARCHAR
);

-- Date Dimension
DROP TABLE IF EXISTS date_dim;
CREATE TABLE date_dim (
   date_id SERIAL PRIMARY KEY,
   ts TIMESTAMP,
   month VARCHAR,
   year INT
);

-- Type Dimension
DROP TABLE IF EXISTS type_dim;
CREATE TABLE type_dim (
   type_id SERIAL PRIMARY KEY,
   animal_type VARCHAR
);

-- Outcome Dimension
CREATE TABLE outcome_dim (
   outcome_id SERIAL PRIMARY KEY,
   outcome_type VARCHAR
);

-- Sex Dimension
CREATE TABLE sex_dim (
   sex_id SERIAL PRIMARY KEY,
   sex VARCHAR,
   sterilization VARCHAR
);

-- Outcomes Fact
CREATE TABLE outcomes_fact (
   outcomesfact_key SERIAL PRIMARY KEY,
   animal_dim_key INT,
   date_id INT,
   outcome_id INT,
   type_id INT,
   sex_id INT,
   FOREIGN KEY (animal_dim_key) REFERENCES animal_dim(animal_dim_key),
   FOREIGN KEY (date_id) REFERENCES Date_dim(date_id),
   FOREIGN KEY (outcome_id) REFERENCES Outcome_dim(outcome_id),
   FOREIGN KEY (type_id) REFERENCES Type_dim(type_id),
   FOREIGN KEY (sex_id) REFERENCES Sex_dim(sex_id)
);
