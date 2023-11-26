
-- Animal Dimension
CREATE TABLE IF NOT EXISTS dim_animals (
    animal_id VARCHAR(7) PRIMARY KEY,
    name VARCHAR,
    dob DATE,
    breed VARCHAR,
    color VARCHAR
);

-- Date Dimension
CREATE TABLE IF NOT EXISTS dim_dates (
    date_id VARCHAR(8) PRIMARY KEY,
    date DATE NOT NULL,
    year INT2  NOT NULL,
    month INT2  NOT NULL,
    day INT2  NOT NULL
);

-- Type Dimension
DROP TABLE IF EXISTS dim_type;
CREATE TABLE dim_type (
   type_id SERIAL PRIMARY KEY,
   animal_type VARCHAR
);

-- Outcome Dimension
CREATE TABLE IF NOT EXISTS dim_outcome_types (
    outcome_type_id INT PRIMARY KEY,
    outcome_type VARCHAR NOT NULL
);

-- Sex Dimension
CREATE TABLE IF NOT EXISTS dim_sex (
   sex_id INT PRIMARY KEY,
   sex VARCHAR ,
   sterilization VARCHAR
);

-- Outcomes Fact
CREATE TABLE fct_outcomes (
    outcome_id SERIAL PRIMARY KEY,
    animal_id VARCHAR(7) NOT NULL,
    date_id VARCHAR(8) NOT NULL,
    time TIME NOT NULL,
    outcome_type_id INT NOT NULL,
    type_id INT,
    sex_id INT,
    outcome_subtype VARCHAR,
    FOREIGN KEY (animal_id) REFERENCES dim_animals(animal_id),
    FOREIGN KEY (date_id) REFERENCES dim_dates(date_id),
    FOREIGN KEY (outcome_type_id) REFERENCES dim_outcome_types(outcome_type_id),
    FOREIGN KEY (type_id) REFERENCES dim_type(type_id),
    FOREIGN KEY (sex_id) REFERENCES dim_sex(sex_id)
);
