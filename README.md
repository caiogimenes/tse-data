# TSE Client API

Este projeto fornece uma classe Python para interagir com a API de Dados Abertos do TSE (Tribunal Superior Eleitoral). Ele permite buscar datasets, grupos, tags, e realizar download de arquivos compactados (ZIP) contendo dados em formato CSV para análise posterior.

---

## Funcionalidades

- **Interação com a API do TSE**:
  - Listagem de datasets, grupos e tags.
  - Busca por URLs de arquivos compactados (ZIP) em datasets específicos.
  
- **Gerenciamento de Dados**:
  - Download de arquivos ZIP e extração de arquivos CSV.
  - Leitura de arquivos CSV para objetos `pandas.DataFrame`.
  - Limpeza e organização de diretórios.

---

## Requisitos

Certifique-se de que você tem os seguintes requisitos instalados:

- Python 3.7 ou superior
- Bibliotecas listadas no `requirements.txt`:
  - `requests`
  - `pandas`
  - `sqlite3`

Instale as dependências com o comando:

```bash
pip install -r requirements.txt
```

---

## Uso

1. **Inicialização do Cliente**:
   Instancie o cliente para começar a interagir com a API do TSE.

   ```python
   from tse_client import TseClient

   client = TseClient()
   print(client)  # <Class TSEClient | interage com API https://dadosabertos.tse.jus.br/api/3/>
   ```

2. **Listagem de Datasets, Grupos e Tags**:
   ```python
   datasets = client.get_dataset_set()
   groups = client.get_groups_set()
   tags = client.get_tag_set()
   ```

3. **Download de Datasets**:
   - Para fazer download de todos os datasets:
     ```python
     client.download_datasets(client.urls['nome_dataset'])
     ```

   - Para encontrar arquivos dentro de um dataset com base em um padrão:
     ```python
     matched_urls = client.find_dataset('nome_dataset', 'padrao_regex')
     client.download_datasets(matched_urls)
     ```

4. **Carregamento de Dados em DataFrames**:
   Após o download, você pode carregar os dados em DataFrames para análise:

   ```python
   dataframes = client.load_data_from_dirs(paths=['caminho_para_arquivos'])
   ```

5. **Remoção de Dados Baixados**:
   Limpe o diretório de trabalho quando necessário:
   ```python
   client.remove_data()
   ```

---

## Estrutura do Projeto

```
├── tse_client.py        # Código principal da classe TseClient
├── data/                # Diretório onde os dados baixados serão armazenados
├── requirements.txt     # Dependências do projeto
├── README.md            # Documentação do projeto
```

---

## Contribuição

Se desejar contribuir para este projeto, sinta-se à vontade para abrir um pull request ou relatar problemas através da aba Issues.

---

## Licença

Este projeto é distribuído sob a licença MIT. Consulte o arquivo `LICENSE` para mais detalhes.
