# Oil Wells Data Wrangling 

## Project Overview
Python-based ETL pipeline that performs OCR on scanned oil well PDFs, parses well and stimulation data, enriches records through web scraping, stores structured results in MySQL, and renders interactive geospatial visualizations via a web interface through Apache.

## Project Structure

```
ocr-oil-well-data-pipeline/
│
├── src/
│ ├── ocr.py
│ ├── filter_pages.py
│ ├── extract_entities.py
│ ├── llm_clean_extraction.py
│ ├── sql_db.py
│ ├── webscraper_v2.py
│ └── build_geojson.py
│
├── www/
│ ├── index.html
│ └── data/
│       └── wells.geo.json
│
├── data/
│  ├── original_pdfs/
│  ├── ...
│  └── final_outputs/
│
├── .env
├── requirements.txt
└── README.md

```

## Data Pipeline Architecture

1. **OCR** extracts raw text from scanned PDFs.
2. **Page Filtering** removes irrelevant pages.
3. **Entity Extraction** identifies well and stimulation data.
4. **LLM Cleaning** standardizes extracted entities using LLM.
5. **Database Storage** inserts structured records into MySQL.
6. **Web Scraping** enriches database with additional fields.
7. **GeoJSON Builder** converts database records to geospatial format.
8. **Frontend Visualization** renders wells on an interactive map.


## Installation

### 1. Clone the Repository

```
git clone <repository-url>
cd ocr-oil-well-data-pipeline
```

### 2. Install Python Dependencies 

```
pip install -r requirements.txt
```

### 3. Install Tesseract OCR

On Mac: 

```
brew install tesseract
```

Windows: Download from [GitHub Tesseract releases](https://github.com/tesseract-ocr/tesseract)

## Setup 

### Google API Key for Gemini LLM 

Add your Google API Key to .env file.

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

Ensure that `www/index.html` and `www/data/wells.geo.json` exist (copy files/folders over to Apache's Document Root)
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
