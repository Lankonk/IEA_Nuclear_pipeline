CREATE TABLE IF NOT EXISTS dim_facilities (
    facility_id VARCHAR(50) PRIMARY KEY,
    plant_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS fact_facility_outages (
    period DATE,
    facility_id VARCHAR(50) REFERENCES dim_facilities(facility_id),
    outage DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS fact_generator_outages (
    period DATE,
    facility_id VARCHAR(50) REFERENCES dim_facilities(facility_id),
    generator VARCHAR(100),
    outage DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS fact_us_outages (
    period DATE,
    outage DOUBLE PRECISION
);