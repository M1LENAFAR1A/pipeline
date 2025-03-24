import os

from app.core.logger import logger as main_logger
import pandas as pd

logger = main_logger

# Palavras que indicam valores nulos ou sem identificação
null_values = ['NI', 'Não identificado', 'Unidentified', 'NA', 'Indet', 'Indefinido', 'Sem Informações', 'nan']


# Função para verificar se o valor é considerado nulo ou sem identificação
def is_null_value(value):
    if pd.isna(value) or str(value).strip() in null_values:
        return True
    return False


# Função para mapear colunas de classe, ordem, família, gênero e espécie
def map_columns(df, taxonomy_df):
    mapped_columns = {}
    species_set = set(taxonomy_df['Especie'].dropna())
    genus_set = set(taxonomy_df['Genero'].dropna())
    family_set = set(taxonomy_df['Familia'].dropna())
    order_set = set(taxonomy_df['Ordem'].dropna())
    class_set = set(taxonomy_df['Classe'].dropna())

    for column in df.columns:
        column_values = set(df[column].astype(str))
        if column_values & species_set:
            mapped_columns[column] = 'Especie'
        elif column_values & genus_set:
            mapped_columns[column] = 'Genero'
        elif column_values & family_set:
            mapped_columns[column] = 'Familia'
        elif column_values & order_set:
            mapped_columns[column] = 'Ordem'
        elif column_values & class_set:
            mapped_columns[column] = 'Classe'

    return mapped_columns


# Função para buscar informações taxonômicas progressivamente, de espécie a classe
def get_taxonomic_info(name, taxonomy_df):
    if is_null_value(name):
        return None

    name = str(name).strip()

    # Estrutura de busca progressiva
    search_order = ['Especie', 'Genero', 'Familia', 'Ordem', 'Classe']
    taxonomic_info = {'Classe': '', 'Ordem': '', 'Familia': '', 'Genero': '', 'Especie': ''}

    for level in search_order:
        row = taxonomy_df[taxonomy_df[level] == name]
        if not row.empty:
            for col in taxonomic_info:
                if not pd.isna(row.iloc[0][col]):
                    taxonomic_info[col] = row.iloc[0][col]
            # Parar após encontrar a primeira correspondência
            break

    # Se nenhuma informação for preenchida, retornar o nome original apenas na coluna de espécie
    if not any(taxonomic_info.values()):
        taxonomic_info['Especie'] = name

    return taxonomic_info


# Função para padronizar colunas e registrar espécies faltantes
def standardize_table(df, mapped_columns, taxonomy_df, missing_species, file_name):
    df['pad_Class'] = ''
    df['pad_Order'] = ''
    df['pad_Family'] = ''
    df['pad_Genus'] = ''
    df['pad_Species'] = ''

    # Processar colunas mapeadas para preencher colunas padronizadas
    for column, column_type in mapped_columns.items():
        if column_type == 'Especie':
            logger.info(f"Coluna de espécies identificada: {column}")
            for i, name in df[column].astype(str).items():
                if is_null_value(name):
                    continue

                # if it is not null, strip the string to remove unwanted whitespaces
                name = str(name).strip()

                taxonomic_info = get_taxonomic_info(name, taxonomy_df)

                # Se não encontrar correspondência para nenhum nível, registrar como faltante
                if taxonomic_info['Classe'] == '' and taxonomic_info['Especie'] == name:
                    missing_species.append({'Species': name, 'File': file_name})
                else:
                    # Preencher as colunas padronizadas com as informações taxonômicas encontradas
                    df.at[i, 'pad_Class'] = taxonomic_info['Classe']
                    df.at[i, 'pad_Order'] = taxonomic_info['Ordem']
                    df.at[i, 'pad_Family'] = taxonomic_info['Familia']
                    df.at[i, 'pad_Genus'] = taxonomic_info['Genero']
                    df.at[i, 'pad_Species'] = taxonomic_info['Especie']
    return df


# Função para processar todas as abas de uma tabela
def process_table(file_path, output_folder, taxonomy_df, missing_species) -> str | None:
    try:
        logger.info(f"Processando arquivo: {file_path}")
        if file_path.endswith('.csv'):
            try:
                df = pd.read_csv(file_path, encoding='utf-8', engine='python')
            except UnicodeDecodeError:
                df = pd.read_csv(file_path, encoding='ISO-8859-1', engine='python')
            sheet_name = 'Sheet1'

            mapped_columns = map_columns(df, taxonomy_df)
            df = standardize_table(df, mapped_columns, taxonomy_df, missing_species, file_path)

            output_path = str(os.path.basename(file_path)).replace('.csv', f'_{sheet_name}.csv')
            output_file = os.path.join(output_folder, output_path)
            df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"Processed {output_file}")
            return output_file
        else:
            xls = pd.ExcelFile(file_path)
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                mapped_columns = map_columns(df, taxonomy_df)
                df = standardize_table(df, mapped_columns, taxonomy_df, missing_species, file_path)
                output_file = os.path.join(output_folder, f"{os.path.basename(file_path)}_{sheet_name}.csv")
                df.to_csv(output_file, index=False, encoding='utf-8-sig')
                logger.info(f"Processed {output_file}")
                return output_file

    except Exception as e:
        logger.error(f"Erro no arquivo {file_path}: {e}")
        return None


# Função principal
def apply_taxonomy_transformation(file_path, output_folder, taxonomy_file) -> str | None:
    taxonomy_df = pd.read_csv(taxonomy_file)
    missing_species = []

    process_table_response = process_table(file_path, output_folder, taxonomy_df, missing_species)

    if missing_species:
        missing_species_df = pd.DataFrame(missing_species)
        missing_species_file = os.path.join(output_folder, 'missing_species.csv')
        missing_species_df.to_csv(missing_species_file, index=False, encoding='utf-8-sig')
        logger.info(f"Espécies faltantes exportadas para {missing_species_file}")
    else:
        logger.info("Nenhuma espécie faltante identificada.")

    return process_table_response
