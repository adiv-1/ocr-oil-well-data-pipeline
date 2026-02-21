import mysql.connector
from mysql.connector import Error
import json
from pathlib import Path

# -----------------------------
# Functions to create MySQL database and appropriate tables
# -----------------------------

def create_database():
    connection = mysql.connector.connect(
        host="localhost",
        user="devuser",
        password=""
    )

    cursor = connection.cursor()
    cursor.execute("DROP DATABASE IF EXISTS oil_wells_db;")
    cursor.execute("CREATE DATABASE oil_wells_db;")
    print("Database created (or exists).")

    cursor.close()
    connection.close()


def create_tables():
    connection = mysql.connector.connect(
        host="localhost",
        user="devuser",
        password="",
        database="oil_wells_db"
    )

    cursor = connection.cursor()

    # Wells table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wells (
        api_number VARCHAR(20) PRIMARY KEY,
        well_name VARCHAR(255),
        operator VARCHAR(255),
        county VARCHAR(100),
        township_range VARCHAR(50),
        latitude DECIMAL(10,6),
        longitude DECIMAL(10,6)
    );
    """)

    # Stimulation events table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stimulation_events (
    id INT AUTO_INCREMENT PRIMARY KEY,
    api_number VARCHAR(20),
    date_stimulated VARCHAR(50),
    formation VARCHAR(100),
    top_ft FLOAT,
    bottom_ft FLOAT,
    stages INT,
    total_volume FLOAT,
    volume_units VARCHAR(50),
    acid_percent FLOAT,
    lbs_proppant FLOAT,
    max_pressure_psi FLOAT,
    max_rate_bbl_per_min FLOAT,
    FOREIGN KEY (api_number) REFERENCES wells(api_number)
        ON DELETE CASCADE
    );
    """)

    # Proppant details table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS proppant_details (
        id INT AUTO_INCREMENT PRIMARY KEY,
        stimulation_event_id INT,
        type VARCHAR(100),
        volume FLOAT,
        FOREIGN KEY (stimulation_event_id) REFERENCES stimulation_events(id)
            ON DELETE CASCADE
    );
    """)

    connection.commit()
    cursor.close()
    connection.close()

    print("Tables created successfully.")


# -----------------------------
# Functions to insert data into MySQL database
# -----------------------------


def insert_well_data():
    data_folder = Path("../data/final_outputs")

    connection = mysql.connector.connect(
        host="localhost",
        user="devuser",
        password="",
        database="oil_wells_db"
    )

    cursor = connection.cursor()

    for file in data_folder.glob("*.json"):
        print(f"Inserting: {file.name}")

        with open(file, "r", encoding="utf-8") as f:
            well = json.load(f)

        # Insert well
        cursor.execute("""
            INSERT INTO wells (
                api_number, well_name, operator, county,
                township_range, latitude, longitude
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                well_name=VALUES(well_name),
                operator=VALUES(operator),
                county=VALUES(county),
                township_range=VALUES(township_range),
                latitude=VALUES(latitude),
                longitude=VALUES(longitude)
        """, (
            well["api_number"],
            well["well_name"],
            well["operator"],
            well["county"],
            well["township_range"],
            well["latitude"],
            well["longitude"]
        ))

        # Insert stimulation events
        for event in well.get("stimulation_events", []):

            date_value = event.get("date_stimulated")

            cursor.execute("""
                INSERT INTO stimulation_events (
                    api_number, date_stimulated, formation,
                    top_ft, bottom_ft, stages,
                    total_volume, volume_units, acid_percent,
                    lbs_proppant, max_pressure_psi,
                    max_rate_bbl_per_min
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                well["api_number"],
                date_value,
                event.get("formation"),
                event.get("top_ft"),
                event.get("bottom_ft"),
                event.get("stages"),
                event.get("total_volume"),
                event.get("volume_units"),
                event.get("acid_percent"),
                event.get("lbs_proppant"),
                event.get("max_pressure_psi"),
                event.get("max_rate_bbl_per_min")
            ))

            stimulation_event_id = cursor.lastrowid

            # Insert proppant breakdown
            for detail in event.get("proppant_breakdown", []):
                cursor.execute("""
                    INSERT INTO proppant_details (
                        stimulation_event_id, type, volume
                    ) VALUES (%s, %s, %s)
                """, (
                    stimulation_event_id,
                    detail.get("type"),
                    detail.get("volume")
                ))

    connection.commit()
    cursor.close()
    connection.close()

    print("All data inserted successfully.")


if __name__ == "__main__":
    create_database()
    create_tables()
    insert_well_data()