import pandas as pd
import sqlite3

# Conectar ao banco de dados SQLite e ler os dados da camada de silver
conn_silver = sqlite3.connect('./data/dados_preco.db')
df_silver = pd.read_sql_query("SELECT * FROM silver", conn_silver)

df_silver['Valor de Venda'] = df_silver['Valor de Venda'].str.replace(',', '.').astype(float)

# Calcular a média de preço para cada produto
df_gold = df_silver.groupby('Produto').agg({'Valor de Venda': 'mean'}).reset_index()
df_gold.columns = ['Produto', 'Preco Medio']

print(df_gold)
# Salvar os dados na camada de gold
# conn_gold = sqlite3.connect('./data/dados_preco.db')

# try:
#     df_gold.to_sql('gold', conn_gold, if_exists='replace', index=False)
#     print("Dados foram salvos na camada de Gold com sucesso.")

# except Exception as e:
#     print(f"Erro ao salvar dados na camada de Gold: {str(e)}")

# finally:
#     conn_gold.close()
#     conn_silver.close()
