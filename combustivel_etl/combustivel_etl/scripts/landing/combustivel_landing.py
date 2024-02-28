import os
import pandas as pd
import requests
import sqlite3
import certifi

# Definir a variável de ambiente SSL_CERT_FILE
os.environ['SSL_CERT_FILE'] = certifi.where()

# URL do arquivo CSV
url = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsan/2024/precos-gasolina-etanol-1.csv"

# Desativar a verificação do certificado SSL
response = requests.get(url)

# Verificar se a solicitação foi bem-sucedida
if response.status_code == 200:
    # Ler o arquivo CSV
    df = pd.read_csv(url,sep=';')

    # Conectar ao banco de dados SQLite na camada de landing
    conn = sqlite3.connect('./data/dados_preco.db')

    # Salvar DataFrame como uma tabela SQLite na camada de landing
    df.to_sql('landing', conn, if_exists='replace', index=False)

    # Fechar a conexão com o banco de dados
    conn.close()

    print("Dados foram salvos na camada de Landing.")
else:
    print("Erro ao baixar o arquivo CSV")
