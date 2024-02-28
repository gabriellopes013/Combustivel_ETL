import pandas as pd
import sqlite3

# Conectar ao banco de dados SQLite e ler os dados da camada de bronze
conn = sqlite3.connect('./data/dados_preco.db')
df_bronze = pd.read_sql_query("SELECT * FROM bronze", conn)

# Selecionar apenas as colunas desejadas
df_silver = df_bronze[['Estado','Regiao', 'Municipio', 'Nome da Rua', 'Produto', 'Valor de Venda', 'Bandeira', 'Revenda', 'Data da Coleta']]

df_filtered = df_silver[(df_silver['Estado'] == 'MG')&(df_silver['Municipio'] == 'BELO HORIZONTE')]

# Salvar os dados na camada de silver
conn_silver = sqlite3.connect('./data/dados_preco.db')
df_filtered.to_sql('silver', conn_silver, if_exists='replace', index=False)
conn_silver.close()

# Fechar a conexão com o banco de dados
conn.close()

print("Dados foram salvos na camada de Silver.")
