import time
import mysql.connector
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup


SEARCH_URL = "https://www.drillingedge.com/search"


def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    return driver

def create_new_fields():
    connection = mysql.connector.connect(
        host="localhost",
        user="devuser",
        password="",
        database="oil_wells_db"
    )

    cursor = connection.cursor()

    try:
        cursor.execute("ALTER TABLE wells ADD COLUMN well_status VARCHAR(100)")
    except:
        pass

    try:
        cursor.execute("ALTER TABLE wells ADD COLUMN well_type VARCHAR(100)")
    except:
        pass

    try:
        cursor.execute("ALTER TABLE wells ADD COLUMN closest_city VARCHAR(100)")
    except:
        pass

    connection.commit()
    cursor.close()
    connection.close()

    print("Columns checked/added successfully.")


def get_wells_from_db():
    connection = mysql.connector.connect(
        host="localhost",
        user="devuser",
        password="",
        database="oil_wells_db"
    )
    cursor = connection.cursor(dictionary=True)
    cursor.execute("SELECT api_number, well_name FROM wells;")
    wells = cursor.fetchall()
    cursor.close()
    connection.close()
    return wells


def update_well_in_db(api_number, data):
    connection = mysql.connector.connect(
        host="localhost",
        user="devuser",
        password="",
        database="oil_wells_db"
    )
    cursor = connection.cursor()

    cursor.execute("""
        UPDATE wells
        SET well_status=%s,
            well_type=%s,
            closest_city=%s
            
        WHERE api_number=%s
    """, (
        data.get("well_status"),
        data.get("well_type"),
        data.get("closest_city"),
        # data.get("oil_produced"),
        # data.get("gas_produced"),
        api_number
    ))

    connection.commit()
    cursor.close()
    connection.close()


def scrape_well_data(driver, api_number):
    driver.get(SEARCH_URL)
    time.sleep(3)

    search_box = driver.find_element(By.NAME, "api_no")
    search_box.clear()
    search_box.send_keys(api_number)
    search_box.send_keys(Keys.RETURN)

    time.sleep(3)

    links = driver.find_elements(By.TAG_NAME, "a")

    well_url = None

    for link in links:
        href = link.get_attribute("href")
        if href and api_number in href:
            well_url = href
            break

    if not well_url:
        print(f"No matching result found for {api_number}")
        return None

    # Navigate directly to well page
    driver.get(well_url)
    time.sleep(3)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    data = {}
    
    try:
        th = soup.find("th", string="Well Status")
        if th:
            td = th.find_next_sibling("td")
            data["well_status"] = td.text.strip() if td else None
        else:
            data["well_status"] = None
    except Exception:
        data["well_status"] = None

    try:
        th = soup.find("th", string="Well Type")
        if th:
            td = th.find_next_sibling("td")
            data["well_type"] = td.text.strip() if td else None
        else:
            data["well_type"] = None
    except Exception:
        data["well_type"] = None
    
    try:
        th = soup.find("th", string="Closest City")
        if th:
            td = th.find_next_sibling("td")
            data["closest_city"] = td.text.strip() if td else None
        else:
            data["closest_city"] = None
    except Exception:
        data["closest_city"] = None
    
    try:
        th = soup.find("th", string="Well Type")
        if th:
            td = th.find_next_sibling("td")
            data["well_type"] = td.text.strip() if td else None
        else:
            data["well_type"] = None
    except Exception:
        data["well_type"] = None

    return data
    # The following two fields (barrels of oil and gas produced) are only available to members of the site.
'''

    try:
        th = soup.find("th", string="Total Oil Prod")
        if th:
            td = th.find_next_sibling("td")
            data["oil_produced"] = td.text.strip() if td else None
        else:
            data["oil_produced"] = None
    except Exception:
        data["oil_produced"] = None
    
    try:
        th = soup.find("th", string="Total Gas Prod")
        if th:
            td = th.find_next_sibling("td")
            data["gas_produced"] = td.text.strip() if td else None
        else:
            data["gas_produced"] = None
    except Exception:
        data["gas_produced"] = None

'''



def main():
    driver = setup_driver()
    wells = get_wells_from_db()

    for well in wells:
        print(f"Scraping: {well['api_number']}")

        try:
            scraped_data = scrape_well_data(
                driver,
                well["api_number"]
            )

            if scraped_data:
                update_well_in_db(well["api_number"], scraped_data)

        except Exception as e:
            print(f"Error scraping {well['api_number']}: {e}")

        time.sleep(2)  

    driver.quit()
    print("Scraping complete.")


if __name__ == "__main__":
    create_new_fields()
    main()