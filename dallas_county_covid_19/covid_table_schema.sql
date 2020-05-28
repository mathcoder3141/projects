CREATE TABLE dallas_covid (
id SERIAL PRIMARY KEY,
new_cases integer NOT NULL,
total_cases integer NOT NULL,
total_deaths integer NOT NULL,
risk_level varchar(25) NOT NULL,
county varchar(255) NOT NULL,
created_at timestamp NULL DEFAULT NULL);