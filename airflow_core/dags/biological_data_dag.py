import datetime
import logging
import os
import sys
import time
from typing import List
from urllib.parse import urljoin, urlencode

import pandas as pd
import pendulum
import requests
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from sqlalchemy import Boolean

logger = logging.getLogger(__name__)

SPREADSHEET_URL = os.getenv('SPREADSHEET_URL', 'https://docs.google.com/spreadsheets/d/1uuRaHuyxNYzVsZrlkcUBGyODw-'
                                               'BYg5zmDtNU1nRA10g/edit?gid=0#gid=0')
ADW_URL = os.getenv('ADW_URL', 'https://animaldiversity.org/accounts/')
RED_LIST_URL = os.getenv('RED_LIST_URL', 'https://www.iucnredlist.org/')

MINIO_URL = os.getenv('MINIO_URL')
MINIO_SECURE = os.getenv('MINIO_SECURE') == "True"
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')
MINIO_BUCKET_NAME = os.getenv('MINIO_BUCKET_NAME')
DATA_LAKE = Variable.get("DATA_LAKE", default_var="minio")

SKIP_ADW = Variable.get("SKIP_ADW", default_var=True)  # flag for data retrieval for adw
SKIP_RED_LIST = Variable.get("SKIP_ADW", default_var=False)  # flag for data retrieval red list

sys.path.append('/opt/airflow')
from connectors.minio import MinioConnector


@dag(
    dag_id="biological_data_dag",
    schedule_interval="0 0 5 * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def BiologicalDataDag():
    def extract_file_id(drive_url: str):
        import re
        # Use a regular expression to find the file ID in the URL
        match = re.search(r'/d/([a-zA-Z0-9_-]+)', drive_url)
        if match:
            return match.group(1)
        else:
            return None

    def create_csv_file(spreadsheet_id: str) -> str:
        url = f'https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export?format=csv'
        response = requests.get(url)
        if response.status_code == 200:
            filepath = os.path.join('/tmp', 'species.csv')
            with open(filepath, 'wb') as f:
                f.write(response.content)
                logger.info('CSV file saved to: {}'.format(filepath))
            return filepath
        else:
            logger.error('Failed to retrieve csv from drive.')

    @task
    def get_data(**kwargs) -> str:
        spreadsheet_id = extract_file_id(SPREADSHEET_URL)
        logger.info(f"file id: {spreadsheet_id}")
        filepath = create_csv_file(spreadsheet_id=spreadsheet_id)
        return filepath

    @task
    def get_species_info(filepath: str) -> List[str]:
        logger.info(f"filepath: {filepath}")
        import csv

        if os.path.exists(filepath):
            with open(filepath, 'r') as file:
                reader = csv.reader(file)
                species = [row[0] for row in reader]

                logger.info('Csv has the following species: fspecies')
                return species
        else:
            logger.error(f'{filepath} not found.')
            return []

    @task
    def extract_data_from_adw(species_list: List[str], skip: Boolean):
        if skip:
            raise AirflowSkipException('Adw data retrieval removed.')

        species_to_store = []
        species_set = set()

        logger.info(f'Number of species to extract data: {len(species_list)}')
        request = 0

        for specie in species_list:
            specie.replace(" ", "_")
            logger.info(f'Specie being extracted: {specie}')

            if not specie in species_set:
                url = urljoin(ADW_URL, f"{specie}/classification/#{specie}")
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                  'Chrome/102.0.0.0 Safari/537.36'
                }

                # Make the GET request
                response = requests.get(url, headers=headers)

                # Check if the request was successful
                if response.status_code == 200:
                    logger.info("Request was successful!")
                    request += 1
                    soup = BeautifulSoup(response.text, 'html.parser')
                    classification_section = soup.find('div', class_='classification well')

                    # Extract all list items within the classification
                    list_items = classification_section.find_all('li')

                    # Loop through each list item and print its details
                    species_object = {}
                    species_object.update({'specie': specie})

                    for item in list_items:
                        rank = item.find('span', class_='rank').text
                        taxon_name = item.find('a', class_='taxon-name').text
                        vernacular_name = item.find('span', class_='vernacular-name').text if item.find('span',
                                                                                                        class_='vernacular-name') else 'N/A'
                        species_object.update({'common_name_adw': vernacular_name})

                        if rank == 'Class':
                            species_object.update({'class_adw': taxon_name})
                        if rank == 'Order':
                            species_object.update({'order_adw': taxon_name})
                        if rank == 'Family':
                            species_object.update({'family_adw': taxon_name})
                        if rank == 'Genus':
                            species_object.update({'genus_adw': taxon_name})
                        if rank == 'Species':
                            species_object.update({'species_adw': taxon_name})

                        information_tab = soup.find('dd', class_='feature-information')

                        logger.info(f'Rank: {rank}')
                        logger.info(f'Taxon Name: {taxon_name}')
                        logger.info(f'Vernacular Name: {vernacular_name}')

                        # to retrive information in information tab if it exists
                        if information_tab:
                            extract_adw_information_tab(species_object, specie)

                        species_set.add(specie)

                        if species_object not in species_to_store:  # to avoid storing repeated
                            species_to_store.append(species_object)

                        if request == 8:  # to limit the amount of species retrieved
                            return species_to_store

                        if request % 10 == 0:
                            time.sleep(10)
                else:
                    logger.error(f"Failed to retrieve content. Status code: {response.status_code}")
            else:
                logger.error(f'The {specie} was already been requested.')
        logger.info(f"species to store {species_to_store}")
        species_set.clear()
        return species_to_store

    def extract_adw_information_tab(species_object: list, specie: str):
        url_info = urljoin(ADW_URL, f"{specie}/")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/102.0.0.0 Safari/537.36'
        }
        info_response = requests.get(url_info, headers=headers)

        if info_response.status_code == 200:
            logger.info("Information tab request was successful")

            soup = BeautifulSoup(info_response.text, 'html.parser')
            range_mass = soup.find('dt', string='Range mass')
            range_length = soup.find('dt', string='Range length')
            primary_diet = soup.find('li', string='Primary Diet')
            key_behaviors = soup.find('li', string='Key Behaviors')
            habitats = soup.find('li', string='Habitat Regions')

            conservation_status_list = [dd.find('span').text for dd in soup.find_all('dd') if
                                        'IUCN Red List' in dd.find_previous('dt').text]

            if range_mass is not None:
                range_mass_value = range_mass.find_next('dd').text
                logger.info(f'Range_mass: {range_mass_value}')
                species_object.update({'range_mass_adw': range_mass_value})

            if range_length is not None:
                range_length_value = range_length.find_next('dd').text
                logger.info(f'Range_length: {range_length_value}')
                species_object.update({'range_lenght_adw': range_length_value})

            if primary_diet is not None:
                primary_diet_value = primary_diet.find_next('a').text
                logger.info(f'primary_diet: {primary_diet_value}')
                species_object.update({'primary_diet_adw': primary_diet_value})

            if key_behaviors is not None:
                key_behaviors_li = key_behaviors.find_next_siblings('li')
                behaviors_values_list = []

                for li in key_behaviors_li:
                    behaviour_tag = li.find('a')
                    if behaviour_tag:
                        behaviour = behaviour_tag.text
                        behaviors_values_list.append(behaviour)

                behaviors_values = ', '.join(behaviors_values_list)
                logger.info(f'Activity patterns: {behaviors_values}')
                species_object.update({'activity_patterns_adw': behaviors_values})

            if habitats is not None:
                habitats_li = habitats.find_next_siblings('li')
                habitats_values_list = []

                for li in habitats_li:
                    habitat_tag = li.find('a')
                    if habitat_tag:
                        habitat = habitat_tag.text
                        habitats_values_list.append(habitat)

                habitats_values = ', '.join(habitats_values_list)
                logger.info(f'Habitats: {habitats_values}')
                species_object.update({'habitat_types_adw': habitats_values})

            if conservation_status_list is not None:
                conservation_status = conservation_status_list[0]
                logger.info(f'Conservation station: {conservation_status}')
                species_object.update({'conservation_status_adw': conservation_status})
            else:
                logger.info('No information tab element available')
        else:
            logger.error('Failed to retrieve content (info). Status code:', info_response.status_code)

    def setup_chrome_driver():
        chrome_options = Options()
        chrome_options.add_argument('--ignore-ssl-errors=yes')
        chrome_options.add_argument('--ignore-certificate-errors')
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--no-sandbox')  # Required for running Chrome as root
        chrome_options.add_argument('--disable-dev-shm-usage')  # Overcome limited resource problems

        chrome_options.binary_location = '/usr/bin/chromium'  # Specify Chromium binary location

        # Use the system-installed ChromeDriver
        service = Service('/usr/bin/chromedriver')

        driver = webdriver.Chrome(
            service=service,
            options=chrome_options
        )

        return driver

    @task(task_id="get_specie_link")
    def get_specie_link(species_list: list, skip):
        if skip:
            raise AirflowSkipException('Adw data retrieval removed.')

        logger.info(f'Number of species to extract data: {len(species_list)}')
        species_links = []
        species_set = set()

        request = 0

        for specie in species_list:
            request += 1

            if not specie in species_set:
                params = {
                    'query': specie,
                    'searchType': 'species'
                }

                url = urljoin(RED_LIST_URL, f'search?{urlencode(params)}')
                logger.info(f'Constructed URL: {url}')

                driver = setup_chrome_driver()

                try:
                    driver.get(url)
                    wait = WebDriverWait(driver, 20)

                    # Wait for the species cards to load
                    wait.until(
                        expected_conditions.presence_of_element_located((By.CSS_SELECTOR, 'div.cards.cards--narrow')))

                    # Finds the first species card link and retrieves the species link
                    first_species_card = wait.until(
                        expected_conditions.presence_of_element_located(
                            (By.CSS_SELECTOR, 'div.cards.cards--narrow a.link--faux'))
                    )
                    specie_link = first_species_card.get_attribute('href')
                    logger.info(f'Link for {specie}: {specie_link}')

                    species_links.append(specie_link)
                    species_set.add(specie)

                    if request == 8:
                        return species_links

                except Exception as e:
                    logger.error("Couldn't retrieve link")
                    logger.error(f'Error occurred: {e}')
                finally:
                    driver.quit()
            else:
                logger.error(f'The {specie} was already been requested.')
        logger.info(f'species links: {species_links}')
        species_set.clear()
        return species_links

    @task
    def extract_data_from_red_list(species_links: list):
        species_to_store = []
        species_object = {}

        for link in species_links:
            driver = setup_chrome_driver()

            try:
                logger.info(f'Species link {link}')
                driver.get(link)
                wait = WebDriverWait(driver, 20)

                # waits for the body of the page to load
                wait.until(
                    expected_conditions.presence_of_element_located((By.TAG_NAME, 'body')))

                wait.until(
                    expected_conditions.presence_of_all_elements_located((By.ID, 'content'))
                )

                extract_species_name(driver, species_object)
                extract_common_name(driver, species_object)
                extract_taxonomy_data(driver, species_object)
                extract_conservation_status(driver, species_object)
                extract_habitat_data(driver, species_object)

                if species_object not in species_to_store:  # to avoid storing repeated
                    species_to_store.append(species_object)

                logger.info('species to store: {species_to_store}')

            except Exception as e:
                logging.error(f'Error occurred: {e}')
                continue
            finally:
                driver.quit()
        return species_to_store

    def extract_taxonomy_data(driver, species_object):
        taxonomy_items = driver.find_elements(By.CSS_SELECTOR, 'div.layout-card--thirds__third')
        if taxonomy_items is not None:
            for item in taxonomy_items:
                rank = item.find_element(By.TAG_NAME, 'h3').text
                taxon_name = item.find_element(By.CSS_SELECTOR, 'p.card__data--accent strong').text

                if rank == 'CLASS':
                    species_object.update({'class_red_list': taxon_name})
                    logger.info(f"Class: {taxon_name}")
                if rank == 'ORDER':
                    species_object.update({'order_red_list': taxon_name})
                    logger.info(f"Order: {taxon_name}")
                if rank == 'FAMILY':
                    species_object.update({'family_red_list': taxon_name})
                    logger.info(f"Family: {taxon_name}")
                if rank == 'GENUS':
                    species_object.update({'genus_red_list': taxon_name})
                    logger.info((f"Genus: {taxon_name}"))
        else:
            logger.error("Taxonomy couldn't be retrieved.")

    def extract_conservation_status(driver, species_object):
        conservation_status = driver.find_element(By.XPATH,
                                                  "//h3[contains(text(), 'IUCN Red List Category and Criteria')]/following-sibling::p[1]")

        if conservation_status is not None:
            conservation_status_value = conservation_status.text
            species_object.update({'conservation_status_red_list': conservation_status_value})
            logger.info(f'Conservation status: {conservation_status_value}')
        else:
            logger.error("Conservation status couldn't be retrieved.")

    def extract_common_name(driver, species_object):
        common_name = driver.find_element(By.CLASS_NAME, 'headline__title')

        if common_name is not None:
            common_name_value = common_name.text
            species_object.update({'common_name_red_list': common_name_value})
            logger.info(f'Common name: {common_name_value}')
        else:
            logger.error("Common name couldn't be retrieved.")

    def extract_species_name(driver, species_object):
        species_name = driver.find_element(By.CLASS_NAME, 'headline__subtitle')

        if species_name is not None:
            species_name_value = species_name.text
            species_object.update({'specie': species_name_value})
            logger.info(f'Species name: {species_name_value}')

        else:
            logger.error("Specie name couldn't be retrieved.")

    def extract_habitat_data(driver, species_object):
        habitat_ecology_card = driver.find_element(By.ID, 'habitat-ecology')

        if habitat_ecology_card is not None:
            habitat = habitat_ecology_card.find_element(By.CLASS_NAME, 'layout-card--split__minor')
            habitat_value = habitat.find_element(By.TAG_NAME, 'a').text
            species_object.update({'habitat_type_red_list': habitat_value})
            logger.info(f'Habitat type: {habitat_value}')
        else:
            logger.error("Habitat type couldn't be retrieved.")

    @task
    def list_to_csv(species_list: list, data_source: str):
        if len(species_list) > 0:

            csv_filename = f'species_data_{data_source}.csv'
            csv_path = os.path.join('/tmp', csv_filename)
            csv_data = pd.DataFrame(species_list)
            csv_data.to_csv(csv_path, index=False)

            logger.info('Data converted to csv.')

            return csv_path
        else:
            logger.error('Species list was empty.')
            return None

    @task
    def save_data(csv_path: str, data_lake: str):
        if data_lake == 'minio':
            connector = MinioConnector(access_key='',)
            current_date = pendulum.now()
            current_year = current_date.year
            current_month = current_date.month
            current_day = current_date.day

            csv_filename = os.path.basename(csv_path)
            csv_name = f"/biological_data/{current_year}/{current_month}/{current_day}/{csv_filename}"

            if os.path.exists(csv_path):
                csv_size = os.stat(csv_path).st_size
                with open(csv_path, 'rb') as csv:
                    connector.upload_data(data=csv, path=csv_name, content_type='text/csv', data_size=csv_size)

                logger.info(f"File {csv_filename} saved in minIo.")
            else:
                raise FileNotFoundError(f"CSV file {csv_filename} not found")

    filepath = get_data()
    species = get_species_info(filepath=filepath)

    # adw list flow
    list_adw = extract_data_from_adw(species, SKIP_ADW)
    csv_path_adw = list_to_csv(list_adw, 'adw')
    save_data_adw = save_data(csv_path=csv_path_adw, data_lake=DATA_LAKE)

    # red list flow
    links = get_specie_link(species, SKIP_RED_LIST)
    list_red = extract_data_from_red_list(links)
    csv_path_red = list_to_csv(list_red, 'red_list')
    save_data_red = save_data(csv_path=csv_path_red, data_lake=DATA_LAKE)

    filepath >> species  # in common flow

    species >> links >> list_red >> csv_path_red >> save_data_red  # red flow

    species >> list_adw >> csv_path_adw >> save_data_adw  # adw flow


dag = BiologicalDataDag()
