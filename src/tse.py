import requests
import random
import os
import io
import zipfile
import re
from pandas import DataFrame, read_csv, read_sql
from typing import Union
import shutil
import sqlite3
import yaml
from pathlib import Path

class Config:
    def __init__(self):
        root = Path().resolve().parent  # Adapte com .parent se necessário
        config_path = root.joinpath('config/config.yaml')
        
        # Validação de existência do arquivo
        if not config_path.exists():
            raise FileNotFoundError(f"Arquivo de configuração não encontrado: {config_path}")
        
        # Carregando configurações
        self.config = self.load_config(config_path)
    
    def load_config(self, file_path):
        with open(file_path, 'r') as file:
            return yaml.safe_load(file)
        
class TseClient(Config):
    def __init__(self):
        super().__init__()
        self.session = requests.Session()
        self.datasets = self.get_dataset_set()
        self.groups = self.get_groups_set()
        self.tags = self.get_tag_set()
        self.urls = self.store_urls()
        self.subsets = None
        
        if not os.path.exists(self.config["paths"]["raw_data"]):
            os.makedirs(self.config["paths"]["raw_data"])
            

    def __repr__(self):
        return f'<Class TSEClient | interage com API {self.config["api"]["base_url"]}>'
    
    def get_dataset_set(self, by: dict = None, param: str = None) -> set:
        if not by:
            response = self.session.get(
                url= self.config["api"]["base_url"] + self.config["api"]["package_list"]
            )
            return set(response.json()['result'])
        elif by == 'group':
            params = {
                'id': param,
                'include_datasets': True
            }
            response = self.session.get(
                url= self.config["api"]["base_url"] + self.config["api"]["group_show"],
                params = params
            )
            datasets = response.json().get('result', {}).get('packages', [])
            f_datasets = [data.get('name') for data in datasets if data.get('name')]
            return set(f_datasets)
    
    def get_groups_set(self) -> set:
        response = self.session.get(
            url= self.config["api"]["base_url"] + self.config["api"]["group_list"]
        )        
        return set(response.json()['result'])
    
    def get_tag_set(self) -> set:
        response = self.session.get(
            url= self.config["api"]["base_url"] + self.config["api"]["tag_list"]
        )        
        return set(response.json()['result'])
    
    
    def store_urls(self) -> dict:
        return {dataset: self.get_dataset_urls(dataset) for dataset in self.datasets}
        
    def get_dataset_urls(self, dataset_name: str) -> set:
        params = {
            'id': dataset_name
        }

        response = self.session.get(
            url= self.config["api"]["base_url"] + self.config["api"]["package_show"],
            params=params
        )
        resources = response.json().get('result', {}).get('resources', [])
        urls = [resource['url'] for resource in resources if 'url' in resource]
        zip_urls = [url for url in urls if url.endswith('.zip')]
        
        return set(zip_urls)
    
    @classmethod
    def _filename_from_url(cls, url) -> str:
        try:
            name = re.split(r'[/.]', url)[-2]
            return name
        except Exception:
            return ''
        
    def download_single_dataset(self, url) -> str:
        file_path = os.path.join(self.config["paths"]["raw_data"], self._filename_from_url(url))
        
        if os.path.exists(file_path):
            print(f'Files downloaded to {file_path}')
            return file_path
        else:
            try:
                response = self.session.get(url, timeout=240)
                response.raise_for_status()
                file_object = io.BytesIO(response.content)
                with zipfile.ZipFile(file_object) as file:
                    csv_files = [f for f in file.namelist() if f.endswith('.csv')]
                    file.extractall(file_path, csv_files)
                print(f'Files downloaded to {file_path}')
                return file_path
            except Exception as e:
                print(f'Erro {e}. {type(e)}')

    def download_datasets(self, urls: Union[set, list]):
        return list(filter(None, [self.download_single_dataset(url) for url in urls]))
    
    def get_subsets(self) -> dict:
        self.subsets = {dataset: set(map(self._filename_from_url, urls)) for dataset, urls in self.urls.items()}
        return self.subsets
            
    def find_dataset(self, dataset: str, pattern: str) -> set:
        assert dataset in self.datasets, 'Dataset must be in datasets set'
        
        dataset_urls = self.urls.get(dataset, set())
        return set(filter(lambda x: re.search(pattern, x), dataset_urls))
            
    @staticmethod
    def sample_from_list(str_set: Union[set, list]) -> str:
        return random.choice(list(str_set))
    
    @staticmethod
    def get_files_from_dirs(paths: Union[set, list]) -> list:
        file_list = []
        for path in paths:
            if os.path.isdir(path):
                for f in os.listdir(path):
                    file_list.append(os.path.join(path, f))
        return file_list
    
    def load_data_from_dirs(self, paths: Union[set, list]) -> dict[str, DataFrame]:
        files = self.get_files_from_dirs(paths)
        data = {}
        for file in files:
            try:
                # Extract the filename without extension
                filename = re.split(r'[\\.]', file)[-2]
                data[filename] = read_csv(file, encoding='latin-1', delimiter=';')
            except Exception as e:
                print(f'Erro ao ler {file}: {e}')
        return data
    
    def remove_data(self, path = None):
        if path is None:
            path = self.config["paths"]["raw_data"]
            
        for folder in os.listdir(path):
            try:
                shutil.rmtree(os.path.join(path, folder))
            except Exception as e:
                print(f'Erro deletando arquivos {e}. Tipo de erro: {type(e)}')
        
        return f'Diretório {path} esvaziado' if not os.listdir(path) else f'Falha em remover arquivos de {path}'
    
    def df_to_sql(cls, df: DataFrame, table_name: str, dbname: str):
        if not os.path.exists(cls.config["paths"]["sql_data"]):
            os.makedirs(cls.config["paths"]["sql_data"])
        # Conectar ao banco de dados
        dbname = os.path.join(cls.config["paths"]["sql_data"], dbname)
        conn = sqlite3.connect(dbname)
        try:
            # Nome da tabela será o nome do dataframe
            df.to_sql(table_name, conn, if_exists='replace', index=False)  # Cria ou substitui a tabela com o nome do dataframe
            print(f'Tabela "{table_name}" foi criada no banco de dados "{dbname}".')
            return table_name
        
        except Exception as e:
            print(f'Erro {e} ao carregar SQL. {type(e)}')
            return None

        finally:
            # Fechar a conexão
            conn.close()
    
    def listar_tabelas(self, db_name: str):
        # Conectar ao banco de dados
        db_path = os.path.join(self.config['paths']['sql_data'], db_name)
        conn = sqlite3.connect(db_path)
        try:
            cursor = conn.cursor()
            # Executar o comando para listar as tabelas
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tabelas = cursor.fetchall()
            print("Tabelas no banco de dados:")
            for tabela in tabelas:
                print(f"- {tabela[0]}")
            return [tabela[0] for tabela in tabelas]
        
        except Exception as e:
            print(f"Erro ao listar tabelas: {e}")
            return None
        
        finally:
            # Fechar a conexão
            conn.close()


class TseAnalysis(Config):
    def __init__(self, database: dict):
        """
        Tse Analysis
        
        Keyword arguments:
        database -- (dict) {
            'path': database_path: str,
            'alias' name: str
        }
        Return: TseAnalysis object
        """
        super().__init__()
        self.main_database = database.get('path')
        self.alias = database.get('alias')
        self.conn = None
        self.cursor = self._setup_()
        pass
    
    def _setup_(self):
        """
        Setup databases
        
        Keyword arguments:
        attach -- (dict) {
            'path': path: str,
            'alias': name: str
        }
        Return: cursor object
        """
        self.conn = sqlite3.connect(database=self.main_database)
        self.cursor = self.conn.cursor()
        self.cursor.execute(f"ATTACH DATABASE '{self.main_database}' AS {self.alias}")
        
        return self.cursor
    
    def attach_database(self, attach: list[dict]):
        for database in attach:
            alias = database.get('alias')
            path = database.get('path')
            self.cursor.execute(f"ATTACH DATABASE '{path}' AS {alias}")
        
        self.cursor.execute("PRAGMA database_list;")
        databases = self.cursor.fetchall()
        print('Bancos carregados:')
        for index, alias, path in databases:
            print(f'{index}) {alias}')
            print(f'-path: {path}')
            self.cursor.execute(f'SELECT name FROM {alias}.sqlite_master WHERE type="table";')
            tables = self.cursor.fetchall()
            print('--tables: ')
            for tabel in tables:
                print('--', tabel[0])
            print('-----')
    
    def create_index(self, table: str, columns: list):
        str_columns = ', '.join(columns)
        try:
            self.cursor.execute(
                f"""
                    CREATE INDEX IF NOT EXISTS
                        idx_{table}
                    ON
                        {table}({str_columns});
                """
            )
            self.conn.commit()
            return 'Indices criados com sucesso'
        
        except Exception as e:
            print(f'Ocorreu erro {e} criando indices. {type(e)}.')
            
    def _unique_values(self, alias, table, col) -> list:
        query = f"""
            SELECT DISTINCT 
                {col}
            FROM 
                {alias}.{table}
        """
        return read_sql(query, self.conn)[col].to_list()
    
    def _count_cases(self, alias: str, table: str, col: str) -> str:
        unique = self._unique_values(alias, table, col)
        statements = []
        for val in unique:
            col_alias = (col.lower() + '_' + val.lower()).replace(' ', '_')
            statements.append(f'SUM(CASE WHEN {col} = "{val}" THEN QT_ELEITORES_PERFIL ELSE 0 END) AS {col_alias}')
        
        return ','.join(statements)   
            
    def generate_stats_query(self, alias, table, cols) -> str:
        counts = [self._count_cases(alias, table, col) for col in cols]
        str_counts = ','.join(counts)
        query = f"""
            SELECT
                CD_MUNICIPIO as municipio,
                NR_LOCAL_VOTACAO as local,
                NR_ZONA as zona,
                NR_SECAO as secao,
                {str_counts}
            FROM
                {alias}.{table}
            GROUP BY
                CD_MUNICIPIO, NR_LOCAL_VOTACAO, NR_ZONA, NR_SECAO
        """
        return query