import pandas as pd
import sqlite3

# Conectar ao banco de dados SQLite e ler os dados da camada de landing
conn = sqlite3.connect('./data/dados_preco.db')
df_landing = pd.read_sql_query("SELECT * FROM landing", conn)

# Fazer transformações nos dados (exemplo: renomear colunas)
df_bronze = df_landing.rename(columns={"Estado - Sigla": "Estado", "Regiao - Sigla": "Regiao"})

# Contagem de linhas em df_bronze
count_bronze = df_bronze.shape[0]

# Remover duplicatas com base em várias colunas
df_bronze_sem_duplicatas = df_bronze.drop_duplicates(subset=['Valor de Venda','CNPJ da Revenda', 'Estado','Municipio', 'Bandeira', 'Produto', 'Revenda', 'Data da Coleta'])

# Contagem de linhas em df_bronze_sem_duplicatas
count_bronze_sem_duplicatas = df_bronze_sem_duplicatas.shape[0]

# Salvar os dados na camada de bronze
conn_bronze = sqlite3.connect('./data/dados_preco.db')

try:
    df_bronze_sem_duplicatas.to_sql('bronze', conn_bronze, if_exists='replace', index=False)
    print("Dados foram salvos na camada de Bronze com sucesso.")
    if count_bronze != count_bronze_sem_duplicatas:
        print("Foram removidas duplicatas.")
    else:
        print("Não houve duplicatas para remover.")
    print("Contagem de linhas em df_bronze:", count_bronze)
    print("Contagem de linhas em df_bronze_sem_duplicatas:", count_bronze_sem_duplicatas)

except Exception as e:
    print(f"Erro ao salvar dados na camada de Bronze: {str(e)}")

finally:
    conn_bronze.close()
    conn.close()
