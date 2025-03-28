from datetime import datetime

import pandas as pd

# Atividade 01 e 02
arquivo = "C:/Users/Julio/Downloads/dados_vendas.csv"
df = pd.read_csv(arquivo, encoding='ISO-8859-1', sep=';')

df.info()
print('\n', df.describe())
df['data_pedido'] = pd.to_datetime(df['data_pedido'], format='%d/%m/%Y', errors='coerce')
df['data_cadastro_produto'] = pd.to_datetime(df['data_cadastro_produto'], format='%d/%m/%Y', errors='coerce')
df['data_cadastro_cliente'] = pd.to_datetime(df['data_cadastro_cliente'], format='%d/%m/%Y', errors='coerce')
df['data_inauguracao'] = pd.to_datetime(df['data_inauguracao'], format='%d/%m/%Y', errors='coerce')

# Atividade 03
df.nome_fornecedor = df.nome_fornecedor.fillna('Não identificado')
df.valor_desconto = df.valor_desconto.transform(lambda x: x.fillna(x.mean()))
df.preco_unitario = df.groupby('id_produto')['preco_unitario'].transform(lambda x: x.fillna(x.mean()))
df['quantidade'] = df['quantidade'].abs()
df.info()


