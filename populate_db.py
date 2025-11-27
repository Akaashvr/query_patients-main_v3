import os
import psycopg2
from psycopg2 import extras
import csv
from pathlib import Path
import time

from utils import get_db_url


STAGING_CREATE_SQL = """
-- Drop existing tables if they exist (in correct order due to foreign keys)
DROP TABLE IF EXISTS admission_lab_results CASCADE;
DROP TABLE IF EXISTS admission_primary_diagnoses CASCADE;
DROP TABLE IF EXISTS admissions CASCADE;
DROP TABLE IF EXISTS patients CASCADE;
DROP TABLE IF EXISTS lab_tests CASCADE;
DROP TABLE IF EXISTS diagnosis_codes CASCADE;
DROP TABLE IF EXISTS lab_units CASCADE;
DROP TABLE IF EXISTS languages CASCADE;
DROP TABLE IF EXISTS marital_statuses CASCADE;
DROP TABLE IF EXISTS races CASCADE;
DROP TABLE IF EXISTS genders CASCADE;
DROP TABLE IF EXISTS stage_labs CASCADE;
DROP TABLE IF EXISTS stage_diagnoses CASCADE;
DROP TABLE IF EXISTS stage_admissions CASCADE;
DROP TABLE IF EXISTS stage_patients CASCADE;

-- Staging tables
CREATE TABLE stage_patients (
    PatientID                              TEXT, 
    PatientGender                          TEXT, 
    PatientDateOfBirth                     TIMESTAMP, 
    PatientRace                            TEXT, 
    PatientMaritalStatus                   TEXT, 
    PatientLanguage                        TEXT, 
    PatientPopulationPercentageBelowPoverty TEXT
);

CREATE TABLE stage_admissions (
    PatientID                              TEXT, 
    AdmissionID                            TEXT, 
    AdmissionStartDate                     TIMESTAMP, 
    AdmissionEndDate                       TIMESTAMP
);

CREATE TABLE stage_diagnoses (
    PatientID                              TEXT, 
    AdmissionID                            TEXT, 
    PrimaryDiagnosisCode                   TEXT, 
    PrimaryDiagnosisDescription            TEXT
);

CREATE TABLE stage_labs (
    PatientID                              TEXT, 
    AdmissionID                            TEXT, 
    LabName                                TEXT, 
    LabValue                               TEXT,
    LabUnits                               TEXT,
    LabDateTime                            TIMESTAMP
);

-- Lookup tables
CREATE TABLE genders (
    gender_id   SERIAL PRIMARY KEY,
    gender_desc TEXT NOT NULL UNIQUE
);

CREATE TABLE races (
    race_id     SERIAL PRIMARY KEY,
    race_desc   TEXT NOT NULL UNIQUE
);

CREATE TABLE marital_statuses (
    marital_status_id   SERIAL PRIMARY KEY,
    marital_status_desc TEXT NOT NULL UNIQUE
);

CREATE TABLE languages (
    language_id SERIAL PRIMARY KEY,
    language_desc TEXT NOT NULL UNIQUE
);

CREATE TABLE lab_units (
    unit_id     SERIAL PRIMARY KEY,
    unit_string TEXT NOT NULL UNIQUE
);

CREATE TABLE lab_tests (
    lab_test_id SERIAL PRIMARY KEY,
    lab_name    TEXT NOT NULL UNIQUE,
    unit_id     INTEGER NOT NULL,
    FOREIGN KEY (unit_id) REFERENCES lab_units(unit_id)
);

CREATE TABLE diagnosis_codes (
    diagnosis_code        TEXT PRIMARY KEY,
    diagnosis_description TEXT NOT NULL
);

-- Core tables
CREATE TABLE patients (
    patient_id     TEXT PRIMARY KEY,
    patient_gender INTEGER,
    patient_dob    TIMESTAMP NOT NULL,
    patient_race   INTEGER,
    patient_marital_status INTEGER,
    patient_language INTEGER,
    patient_population_pct_below_poverty REAL,
    FOREIGN KEY (patient_gender) REFERENCES genders(gender_id),
    FOREIGN KEY (patient_race) REFERENCES races(race_id),
    FOREIGN KEY (patient_marital_status) REFERENCES marital_statuses(marital_status_id),
    FOREIGN KEY (patient_language) REFERENCES languages(language_id)
);

CREATE TABLE admissions (
    patient_id      TEXT NOT NULL,
    admission_id    INTEGER NOT NULL,
    admission_start TIMESTAMP NOT NULL,
    admission_end   TIMESTAMP,
    PRIMARY KEY (patient_id, admission_id),
    FOREIGN KEY (patient_id) REFERENCES patients(patient_id)
);

CREATE TABLE admission_primary_diagnoses (
    patient_id     TEXT NOT NULL,
    admission_id   INTEGER NOT NULL,
    diagnosis_code TEXT NOT NULL,
    PRIMARY KEY (patient_id, admission_id),
    FOREIGN KEY (patient_id, admission_id) REFERENCES admissions(patient_id, admission_id),
    FOREIGN KEY (diagnosis_code) REFERENCES diagnosis_codes(diagnosis_code)
);

CREATE TABLE admission_lab_results (
    patient_id    TEXT NOT NULL,
    admission_id  INTEGER NOT NULL,
    lab_test_id   INTEGER NOT NULL,
    lab_value     REAL,
    lab_datetime  TIMESTAMP NOT NULL,
    FOREIGN KEY (patient_id, admission_id) REFERENCES admissions(patient_id, admission_id),
    FOREIGN KEY (lab_test_id) REFERENCES lab_tests(lab_test_id),
    UNIQUE (patient_id, admission_id, lab_test_id, lab_datetime)
);
"""

FILES = {
    "patients": {
        "filename": "PatientCorePopulatedTable.txt",
     },
    "admissions": {
        "filename": "AdmissionsCorePopulatedTable.txt",
     },
    "diagnoses": {
        "filename": "AdmissionsDiagnosesCorePopulatedTable.txt",
     },
    "labs": {
        "filename": "LabsCorePopulatedTable.txt",
        "batch_size": 100_000,
     }
}

EXPECTED_COLUMNS = {
    "patients": [
        "PatientID",
        "PatientGender",
        "PatientDateOfBirth",
        "PatientRace",
        "PatientMaritalStatus",
        "PatientLanguage",
        "PatientPopulationPercentageBelowPoverty",
    ],
    "admissions": [
        "PatientID",
        "AdmissionID",
        "AdmissionStartDate",
        "AdmissionEndDate",
    ],
    "diagnoses": [
        "PatientID", 
        "AdmissionID", 
        "PrimaryDiagnosisCode", 
        "PrimaryDiagnosisDescription",               
    ],
    "labs": [
        "PatientID",
        "AdmissionID",
        "LabName",
        "LabValue",
        "LabUnits",
        "LabDateTime",
    ]
}

def load_tsv_to_stage(conn, filepath, stage_table, expected_columns, batch_size=5_000):
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {filepath}")

    with path.open("r", encoding="utf-8-sig") as csvfile:
        csv_reader = csv.DictReader(csvfile, delimiter='\t')
        # validate columns
        missing = sorted(set(expected_columns) - set(csv_reader.fieldnames))
        if missing:
            raise ValueError(f"{filepath} missing expected columns: {missing}")

        placeholders = ", ".join(["%s"] * len(expected_columns))
        sql = f"INSERT INTO {stage_table} ({', '.join(expected_columns)}) VALUES ({placeholders})"
        rows = []
        row_count = 0 
        total_count = 0
        cursor = conn.cursor()
        
        cursor.execute(f"DELETE FROM {stage_table}")
        conn.commit()
        print(f"Cleaned up rows from {stage_table}")
        
        log_template = "Inserted another batch of {:,} rows; total: {:,}"
        for row in csv_reader:
            rows.append([row.get(c, None) for c in expected_columns])
            row_count += 1

            if row_count == batch_size:
                extras.execute_batch(cursor, sql, rows)
                conn.commit()
                total_count += len(rows)
                row_count = 0 
                rows = []  
                print(log_template.format(batch_size, total_count))

        if rows:
            extras.execute_batch(cursor, sql, rows)
            conn.commit()
            total_count += len(rows)  
            print(log_template.format(len(rows), total_count))

        cursor.close()
        print(f"Finished loading data into {stage_table}")


def build_dimensions(conn):
    cur = conn.cursor()
    
    # Genders
    cur.execute("""
        INSERT INTO genders(gender_desc)
        SELECT DISTINCT PatientGender FROM stage_patients 
        WHERE PatientGender IS NOT NULL AND PatientGender <> ''
        ON CONFLICT (gender_desc) DO NOTHING;
    """)
    
    # Races
    cur.execute("""
        INSERT INTO races(race_desc)
        SELECT DISTINCT PatientRace FROM stage_patients 
        WHERE PatientRace IS NOT NULL AND PatientRace <> ''
        ON CONFLICT (race_desc) DO NOTHING;
    """)
    
    # Marital statuses
    cur.execute("""
        INSERT INTO marital_statuses(marital_status_desc)
        SELECT DISTINCT PatientMaritalStatus FROM stage_patients 
        WHERE PatientMaritalStatus IS NOT NULL AND PatientMaritalStatus <> ''
        ON CONFLICT (marital_status_desc) DO NOTHING;
    """)
    
    # Languages
    cur.execute("""
        INSERT INTO languages(language_desc)
        SELECT DISTINCT PatientLanguage FROM stage_patients 
        WHERE PatientLanguage IS NOT NULL AND PatientLanguage <> ''
        ON CONFLICT (language_desc) DO NOTHING;
    """)
    
    # Lab units
    cur.execute("""
        INSERT INTO lab_units(unit_string)
        SELECT DISTINCT LabUnits FROM stage_labs 
        WHERE LabUnits IS NOT NULL AND LabUnits <> ''
        ON CONFLICT (unit_string) DO NOTHING;
    """)
    
    # Lab tests (LabName -> Unit)
    cur.execute("""
        INSERT INTO lab_tests(lab_name, unit_id)
        SELECT DISTINCT s.LabName, u.unit_id
        FROM stage_labs s
        JOIN lab_units u ON u.unit_string = s.LabUnits
        WHERE s.LabName IS NOT NULL AND s.LabName <> ''
        ON CONFLICT (lab_name) DO NOTHING;
    """)
    
    # Diagnosis codes
    cur.execute("""
        INSERT INTO diagnosis_codes(diagnosis_code, diagnosis_description)
        SELECT DISTINCT PrimaryDiagnosisCode, PrimaryDiagnosisDescription
        FROM stage_diagnoses
        WHERE PrimaryDiagnosisCode IS NOT NULL AND PrimaryDiagnosisCode <> ''
        ON CONFLICT (diagnosis_code) DO NOTHING;
    """)
    
    conn.commit()
    cur.close()
    print("Dimension tables populated")


def load_entities(conn):
    cur = conn.cursor()
    
    # Patients
    cur.execute("""
        INSERT INTO patients (
            patient_id, patient_gender, patient_dob, patient_race,
            patient_marital_status, patient_language, patient_population_pct_below_poverty
        )
        SELECT
            s.PatientID,
            g.gender_id,
            s.PatientDateOfBirth,
            r.race_id,
            m.marital_status_id,
            l.language_id,
            NULLIF(s.PatientPopulationPercentageBelowPoverty, '')::REAL
        FROM stage_patients s
        LEFT JOIN genders g ON g.gender_desc = s.PatientGender
        LEFT JOIN races r ON r.race_desc = s.PatientRace
        LEFT JOIN marital_statuses m ON m.marital_status_desc = s.PatientMaritalStatus
        LEFT JOIN languages l ON l.language_desc = s.PatientLanguage
        ON CONFLICT (patient_id) DO NOTHING;
    """)
    
    # Admissions
    cur.execute("""
        INSERT INTO admissions (patient_id, admission_id, admission_start, admission_end)
        SELECT
            s.PatientID,
            s.AdmissionID::INTEGER,
            s.AdmissionStartDate,
            s.AdmissionEndDate
        FROM stage_admissions s
        ON CONFLICT (patient_id, admission_id) DO NOTHING;
    """)
    
    conn.commit()
    cur.close()
    print("Entity tables populated")


def build_facts(conn):
    cur = conn.cursor()
    
    # Primary diagnoses
    cur.execute("""
        INSERT INTO admission_primary_diagnoses (patient_id, admission_id, diagnosis_code)
        SELECT
            s.PatientID,
            s.AdmissionID::INTEGER,
            s.PrimaryDiagnosisCode
        FROM stage_diagnoses s
        JOIN diagnosis_codes d ON d.diagnosis_code = s.PrimaryDiagnosisCode
        ON CONFLICT (patient_id, admission_id) DO NOTHING;
    """)
    
    # Lab results
    cur.execute("""
        INSERT INTO admission_lab_results (
            patient_id, admission_id, lab_test_id, lab_value, lab_datetime
        )
        SELECT
            s.PatientID,
            s.AdmissionID::INTEGER,
            lt.lab_test_id,
            NULLIF(s.LabValue, '')::REAL,
            s.LabDateTime
        FROM stage_labs s
        JOIN lab_tests lt ON lt.lab_name = s.LabName
        ON CONFLICT (patient_id, admission_id, lab_test_id, lab_datetime) DO NOTHING;
    """)
    
    conn.commit()
    cur.close()
    print("Fact tables populated")


# Main execution
if __name__ == "__main__":
    
    DATABASE_URL = get_db_url()
    # Create tables
    print("Creating tables...")
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute(STAGING_CREATE_SQL)
    conn.commit()
    cursor.close()
    conn.close()
    print("Tables created successfully\n")

    # Load staging data
    print("Loading staging data...")
    start_time = time.monotonic()
    conn = psycopg2.connect(DATABASE_URL)
    for name in FILES:
        load_tsv_to_stage(
            conn, 
            FILES[name]["filename"], 
            f"stage_{name}", 
            EXPECTED_COLUMNS[name], 
            FILES[name].get("batch_size", 5_000)
        )
    conn.close()
    end_time = time.monotonic()
    elapsed_time = end_time - start_time
    print(f"\nStaging data loaded. Elapsed time: {elapsed_time:.2f} seconds\n")

    # Build dimensions
    print("Building dimension tables...")
    conn = psycopg2.connect(DATABASE_URL)
    build_dimensions(conn)
    conn.close()

    # Load entities
    print("Loading entity tables...")
    conn = psycopg2.connect(DATABASE_URL)
    load_entities(conn)
    conn.close()

    # Build facts
    print("Building fact tables...")
    conn = psycopg2.connect(DATABASE_URL)
    build_facts(conn)
    conn.close()
    
    print("\n✅ Database migration complete!")