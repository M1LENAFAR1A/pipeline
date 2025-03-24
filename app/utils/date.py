import os
from datetime import datetime
from app.core.logger import logger as main_logger
import pandas as pd

logger = main_logger


# Função para padronizar datas no formato americano (yyyy-mm-dd), ignorando a hora, se houver
def padronizar_data(data):
    try:
        # Remove o horário (T) e o sufixo Z se existirem
        if 'T' in data:
            data = data.split('T')[0]
        if 'Z' in data:
            data = data.replace('Z', '')
        # Tenta converter para o formato %Y-%m-%d
        return datetime.strptime(data, '%Y-%m-%d').strftime('%Y-%m-%d')
    except ValueError:
        try:
            return datetime.strptime(data, '%d/%m/%Y').strftime('%Y-%m-%d')
        except ValueError:
            return data  # Se não puder converter, retorna o valor original


def padronizar_hora(data: str):
    if data.endswith('Z'):
        data = data[:-1]

    # Split by 'T' to separate date and time
    if 'T' in data:
        _, time_part = data.split('T', 1)
        return time_part  # Return everything after 'T'
    else:
        # If there's no 'T', return an empty string or raise an error
        return ""


# Função para concatenar colunas de year, month, day em uma nova coluna "date"
def concatenar_data(row):
    year = str(row.get('year', ''))
    month = str(row.get('month', '')).zfill(2)  # Preenche com zero à esquerda se necessário
    day = str(row.get('day', '')).zfill(2)

    # Se ano existir, cria data; caso contrário, retorna vazio
    if year:
        return f"{year}-{month or '01'}-{day or '01'}"  # Usa '01' se mês ou dia estiverem ausentes
    return ''


# Função para verificar e converter coordenadas
def converter_coordenadas(valor):
    try:
        return float(str(valor).replace(',', '.'))
    except ValueError:
        return None


# Função para processar os arquivos CSV
def processar_csv(df, file_path: str, output_folder: str) -> str | None:
    # df = ler_csv_arquivo(file_path)
    if df is not None:
        # Procura colunas que contenham "date" ou "data" e que não sejam "datasetKey"
        # or observerd_on for Inaturalist API
        data_columns = [col for col in df.columns if
                        ('date' in col.lower() or 'data' in col.lower() or 'observed_on' in col.lower())
                        and col.lower() != 'datasetkey']

        if data_columns:
            logger.info(f"Colunas de data encontradas em {file_path}: {data_columns}")
            for col in data_columns:
                # Cria uma nova coluna com o nome "nomedacoluna_pad"
                data_nova_coluna = f"{col}_pad"
                hora_nova_coluna = f"{col}_hora_pad"
                df[data_nova_coluna] = df[col].apply(lambda x: padronizar_data(str(x)))
                df[hora_nova_coluna] = df[col].apply(lambda x: padronizar_hora(str(x)))

        # Se não encontrar colunas "date" ou "data", verifica se existem "year", "month", "day"
        elif any(col in df.columns for col in ['year', 'month', 'day']):
            logger.info(f"Colunas 'year', 'month' ou 'day' encontradas em {file_path}, criando coluna 'date_pad'")
            df['date_pad'] = df.apply(concatenar_data, axis=1)

        # Verifica colunas de latitude e longitude
        lat_columns = [col for col in df.columns if 'lat' in col.lower() or 'latitude' in col.lower()]
        long_columns = [col for col in df.columns if 'long' in col.lower() or 'longitude' in col.lower()]

        if lat_columns and long_columns:
            logger.info(f"Colunas de coordenadas encontradas em {file_path}: {lat_columns}, {long_columns}")
            # Usa a primeira coluna encontrada de latitude e longitude
            df['Lat_pad'] = df[lat_columns[0]].apply(converter_coordenadas)
            df['Long_pad'] = df[long_columns[0]].apply(converter_coordenadas)

        # Salva o novo arquivo .csv com o prefixo "pad_"
        new_file_name = f"pad_{file_path.split('/')[-1]}"
        df.to_csv(os.path.join(output_folder, new_file_name), encoding='utf-8', index=False)
        logger.info(f"Arquivo processado e salvo: {new_file_name}")
        return new_file_name
    else:
        logger.error(f"Erro ao processar {file_path}")
        return None


# Função para ler CSV com autodetecção de delimitador
def ler_csv_arquivo(file_path):
    try:
        # Tenta diferentes delimitadores (vírgula, ponto e vírgula, tabulação)
        df = pd.read_csv(file_path, sep=",", engine='python', encoding='utf-8')
        logger.info(f"Lendo arquivo CSV: {file_path}")
        return df
    except Exception as e:
        logger.error(f"Erro ao ler {file_path}: {e}")
        return None
