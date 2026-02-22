# Oil Wells Data Wrangling 

## Project Overview
Python-based ETL pipeline that performs OCR on scanned oil well PDFs, parses well and stimulation data, enriches records through web scraping, stores structured results in MySQL, and renders interactive geospatial visualizations via a web interface.

## Setup 

### Google API Key for Gemini LLM 

* Add your Google API Key to .env file

### MySQL Database

The Python scripts create the databases with 3 tables (no need to create them yourself, make sure MySQL is downloaded!). However, in `sql_db.py`, `webscraper_v2.py`, and `build_geojson.py`, the following settings have been set as a default. Adjust according to your database configurations and credentials.

```
host="localhost",
user="devuser",
password=""
```

## Usage

### 1. Process PDF Documents (OCR and Post-Processing)

```
cd src
python ocr.py
python filter_pages.py
python extract_entities.py
python llm_clean_extraction.py
```

### 2. Store Structured JSON Results in MySQL Database

```
python sql_db.py
```

### 3. Webscrape to Add Fields to Database

```
python webscraper_v2.py
```

### 4. Build .GeoJSON File For Web Interface

```
python build_geojson.py
```

### 5. Launch Web Server with Apache
Make sure Apache is downloaded.
```
brew install httpd
```

Ensure that `www/index.html` and `www/data/wells.geo.json` exist (Copy files/folders over to Apache's Document Root)
```
cp -R <path to your www folder e.g. ~/ocr-oil-well-data-pipeline/www/*> /usr/local/var/www/
```

Start Apache.
```
brew services start httpd
```

Open your browser and go to http://localhost:8080. 

## Database Schema 

`wells` Table

* `api_number` VARCHAR(20) PRIMARY KEY
* `well_name` VARCHAR(255)
* `operator` VARCHAR(255)
* `county` VARCHAR(100)
* `township_range` VARCHAR(50)
* `latitude` DECIMAL(10,6)
* `longitude` DECIMAL(10,6)

`stimulation_events` Table

* `id` INT AUTO_INCREMENT PRIMARY KEY
* `api_number` VARCHAR(20)
* `date_stimulated` VARCHAR(50)
* `formation` VARCHAR(100)
* `top_ft` FLOAT
* `bottom_ft` FLOAT
* `stages` INT
* `total_volume` FLOAT
* `volume_units` VARCHAR(50)
* `acid_percent` FLOAT
* `lbs_proppant` FLOAT
* `max_pressure_psi` FLOAT
* `max_rate_bbl_per_min` FLOAT

`proppant_details` Table

* `id` INT AUTO_INCREMENT PRIMARY KEY
* `stimulation_event_id` INT
* `type` VARCHAR(100)
* `volume` FLOAT
