import io
import logging
import os
import urllib.parse
from pathlib import Path

import pandas as pd

from app.connectors.minio import MinioConnector
from app.core.config import settings
from app.schemas.response import MinioResponse, Response
from app.utils.date import processar_csv
from app.utils.taxonomy import apply_taxonomy_transformation

TAXONOMY_FILE = settings.TAXONOMY_FILE


class TransformationPipeline:
    def __init__(self, output_folder, minio_connector: MinioConnector):
        self.output_folder = output_folder.rstrip("/") + "/"  # for urljoin, ensure it ends with a trailing slash
        self.minio_connector = minio_connector
        self.logger = logging.getLogger('app')

    def create_transformation_directory(self):
        os.makedirs(self.output_folder, exist_ok=True)

    def run(self, file_path: str) -> tuple[bool, str]:
        self.logger.info(' [*] Starting pipeline...')
        self.logger.info('Applying date and coordinates transformations')
        date_transformation_response: Response = self.date_transformation(file_path=file_path)
        if date_transformation_response.success:
            self.logger.info("Applying taxonomy transformation")
            new_filename = date_transformation_response.value
            taxonomy_transformation_response = self.taxonomy_transformation(new_filename=new_filename)
            if not taxonomy_transformation_response.success:
                os.remove(urllib.parse.urljoin(self.output_folder, new_filename))
                return False, taxonomy_transformation_response.value

            self.logger.info("Inserting transformed value into Minio")
            self.insert_file_into_minio(file_path=taxonomy_transformation_response.value,
                                        new_filename=f'/transformed/{new_filename}')
            os.remove(urllib.parse.urljoin(self.output_folder, new_filename))
            missing_species_path = urllib.parse.urljoin(self.output_folder, 'missing_species.csv')
            if os.path.exists(missing_species_path):
                self.logger.info(f'Inserting missing species into data lake')
                self.insert_file_into_minio(file_path=missing_species_path,
                                            new_filename='/missing/missing_species.csv')
                os.remove(missing_species_path)
            return True, taxonomy_transformation_response.value
        else:
            self.logger.error("Something went wrong in the transformation")
            self.logger.error(f"{date_transformation_response.value}")
            self.logger.info("Inserting again in the queue")
            return False, date_transformation_response.value

    def date_transformation(self, file_path: str) -> Response:
        object_response: MinioResponse = self.minio_connector.get_object(file_path=file_path)
        if object_response.success:
            df = pd.read_csv(io.BytesIO(object_response.value))

            processing_response = processar_csv(df=df, file_path=file_path, output_folder=self.output_folder)
            return Response(success=True, value=processing_response)
        else:
            return Response(success=False, value=object_response.value)

    def taxonomy_transformation(self, new_filename: str) -> Response:
        transformed_file_path = urllib.parse.urljoin(self.output_folder, new_filename)
        taxonomy_transformation_response = apply_taxonomy_transformation(file_path=transformed_file_path,
                                                                         output_folder=self.output_folder,
                                                                         taxonomy_file=TAXONOMY_FILE)
        if taxonomy_transformation_response is not None:
            return Response(success=True, value=taxonomy_transformation_response)
        else:
            return Response(success=False, value='Something went wrong in the taxonomy transformation')

    def insert_file_into_minio(self, file_path: str, new_filename: str):
        csv_path = Path(file_path)
        if csv_path.exists():
            csv_size = os.stat(csv_path).st_size

            with open(csv_path, 'rb') as csv:
                self.minio_connector.upload_data(file_path=f'{new_filename}',
                                                 data=csv,
                                                 content_type='text/csv',
                                                 file_size=csv_size)
