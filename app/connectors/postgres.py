import json
import logging
from logging import Logger
from typing import List, Optional

import pandas as pd
import psycopg
from pydantic import ValidationError

from app.schemas.data import DadosBaseSchema


class PostgresConnector:
    def __init__(self, host: str, port: int, dbname: str, user: str, password: str, logger: Logger):
        self.db_conn_params = {
            'host': host,
            'port': port,
            'dbname': dbname,
            'user': user,
            'password': password
        }

        self.logger = logger

    def open_csv_file(self, file_path: str) -> dict:
        df = pd.read_csv(file_path)

        # Convert the DataFrame to a JSON string
        json_data = df.to_json(orient='records', indent=4)
        return json.loads(json_data)

    def process_record(self, source: str, record: dict) -> dict:
        validated_record = dict()

        if source == 'GBIF':
            validated_record.update(
                {
                    "class_": record.get('pad_Class'),
                    "order_": record.get('pad_Order'),
                    "family": record.get('pad_Family'),
                    "genus": record.get('pad_Genus'),
                    "species": record.get('pad_Species'),
                    "subspecies": record.get('infraspecificEpithet'),
                    "latitude": record.get('Lat_pad'),
                    "longitude": record.get('Long_pad'),
                    "registration_date": record.get('eventDate_pad'),
                }
            )
        elif source == 'Inaturalist':
            validated_record.update(
                {
                    "class_": record.get('pad_Class'),
                    "order_": record.get('pad_Order'),
                    "family": record.get('pad_Family'),
                    "genus": record.get('pad_Genus'),
                    "species": record.get('pad_Species'),
                    "subspecies": record.get('taxon_name'),
                    "latitude": record.get('Lat_pad'),
                    "longitude": record.get('Long_pad'),
                    "registration_date": record.get('observed_on_pad'),
                }
            )
        else:
            return record

        return validated_record

    def validate_record(self, source: str, record: dict) -> tuple[bool, Optional[tuple]]:
        try:
            if source == 'Inaturalist' or source == 'GBIF':
                # uses the dadosbase schema for validation
                validated = DadosBaseSchema(**record)
                validated_record = (
                    validated.class_,
                    validated.order_,
                    validated.family,
                    validated.genus,
                    validated.species,
                    validated.subspecies,
                    validated.latitude,
                    validated.longitude,
                    validated.registration_date,
                    source
                )
                return True, validated_record
            else:
                # should other tables schema for validation
                # need to be implemented accordingly and then remove the failed validation iteration
                self.logger.error(f'No validation schema available for {source}')
                return False, None
        except ValidationError as e:
            self.logger.error(f"Validation failed: {e}")
            return False, None

    def validate_data(self, source: str, json_data: dict) -> List[tuple]:
        validated_data = []
        number_of_processed_records = 0
        failed_validations = 0
        number_of_records = len(json_data)
        if source == 'Inaturalist' or source == 'GBIF':
            for record in json_data:
                processed_record = self.process_record(source=source, record=record)
                number_of_processed_records += 1
                division_value = number_of_records / 10
                if number_of_processed_records % division_value == 0:
                    self.logger.info(f"Processed {number_of_processed_records} records")

                validated_record = self.validate_record(source=source, record=processed_record)
                if validated_record[0]:
                    validated_data.append(validated_record[1])
                else:
                    failed_validations += 1
        else:
            self.logger.error(f'No validation schema available for {source}')
            raise

        self.logger.info(f"Processed {number_of_processed_records}/{number_of_records} records")
        self.logger.warning(f"Validation failed for {failed_validations} records")
        return validated_data

    def insert_in_table(self, source: str, transformed_values: List[tuple]):
        with psycopg.connect(**self.db_conn_params) as conn:
            # Open a cursor to perform database operations
            with conn.cursor() as cur:
                # Execute a command: this creates a new table if not exists
                if source == 'Inaturalist' or source == 'GBIF':
                    table_name = 'dados_base'
                    create_table_script = f"""
                                                       CREATE TABLE IF NOT EXISTS {table_name} (
                                                           id serial PRIMARY KEY,
                                                           class_ varchar(255),
                                                           order_ varchar(255),
                                                           family varchar(255),
                                                           genus varchar(255),
                                                           species varchar(255),
                                                           subspecies varchar(255),
                                                           latitude float NOT NULL,
                                                           longitude float NOT NULL,
                                                           registration_date timestamp NOT NULL,
                                                           source varchar(24)                                       
                                                       )
                                                   """
                    insertion_script = (
                        f'INSERT INTO {table_name} (class_, order_, family, genus, species, subspecies, '
                        f'latitude, longitude, registration_date, source)'
                        f' VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)')
                else:
                    # TODO add other scripts for database and insertion (other tables)
                    raise

                cur.execute(create_table_script)
                query = insertion_script

                # Execute the query
                cur.executemany(query, transformed_values)

                self.logger.info("Inserted records into the table.")

                # Make the changes to the database persistent
                conn.commit()
