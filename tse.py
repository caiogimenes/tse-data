import requests
import random
import os
import io
import zipfile
import re
from pandas import DataFrame, read_csv
from typing import Union
import shutil

BASE_URL = 'https://dadosabertos.tse.jus.br/api/3/'
PACKAGE_LIST_PATH = 'action/package_list'
GROUP_LIST_PATH = 'action/group_list'
PACKAGE_SHOW_PATH = 'action/package_show'
TAG_LIST_PATH = 'action/tag_list'
GROUP_SHOW_PATH = 'action/group_show'
OUTPUT_FOLDER = 'data'

class TseClient():
    def __init__(self):
        self.session = requests.Session()
        self.datasets = self.get_dataset_set()
        self.groups = self.get_groups_set()
        self.tags = self.get_tag_set()
        self.urls = self.store_urls()
        self.subsets = None
        
        if not os.path.exists(OUTPUT_FOLDER):
            os.makedirs(OUTPUT_FOLDER)

    def __repr__(self):
        return f'<Class TSEClient | interage com API {BASE_URL}>'
    
    def get_dataset_set(self, by: dict = None, param: str = None) -> set:
        if not by:
            response = self.session.get(
                url= BASE_URL + PACKAGE_LIST_PATH
            )
            return set(response.json()['result'])
        elif by == 'group':
            params = {
                'id': param,
                'include_datasets': True
            }
            response = self.session.get(
                url= BASE_URL + GROUP_SHOW_PATH,
                params = params
            )
            datasets = response.json().get('result', {}).get('packages', [])
            f_datasets = [data.get('name') for data in datasets if data.get('name')]
            return set(f_datasets)
    
    def get_groups_set(self) -> set:
        response = self.session.get(
            url= BASE_URL + GROUP_LIST_PATH
        )        
        return set(response.json()['result'])
    
    def get_tag_set(self) -> set:
        response = self.session.get(
            url= BASE_URL + TAG_LIST_PATH
        )        
        return set(response.json()['result'])
    
    
    def store_urls(self) -> dict:
        return {dataset: self.get_dataset_urls(dataset) for dataset in self.datasets}
        
    def get_dataset_urls(self, dataset_name: str) -> set:
        params = {
            'id': dataset_name
        }

        response = self.session.get(
            url= BASE_URL + PACKAGE_SHOW_PATH,
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
        file_path = os.path.join(OUTPUT_FOLDER, self._filename_from_url(url))
        
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
    
    @staticmethod
    def remove_data(path = OUTPUT_FOLDER):
        for folder in os.listdir(path):
            try:
                shutil.rmtree(os.path.join(path, folder))
            except Exception as e:
                print(f'Erro deletando arquivos {e}. Tipo de erro: {type(e)}')
        
        return f'Diretório {path} esvaziado' if not os.listdir(path) else f'Falha em remover arquivos de {path}'