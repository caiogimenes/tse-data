import sqlite3
import os
import pandas as pd

PATH = 'databases'

def df_to_sql(df: pd.DataFrame, table_name: str, dbname: str):
    if not os.path.exists(PATH):
        os.makedirs(PATH)
    # Conectar ao banco de dados
    dbname = os.path.join(PATH, dbname)
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

def listar_tabelas(db_path: str):
    # Conectar ao banco de dados
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
