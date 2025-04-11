import pandas as pd
import numpy as np
import psycopg2
import psycopg2.extras
from datetime import datetime


## instruçao para executar o codigo 
## Nao mudar a conexao com o banco de dados, pode manter as credenciais existentes. 

# ==================================================
# ETAPA 1: EXTRAÇÃO E ANÁLISE EXPLORATÓRIA
# ==================================================

# Carregar o CSV diretamente do GitHub
url = "https://raw.githubusercontent.com/GabrielSique1r4/Pipeline_de_dados/e34d31a12991a44f58a1aa5210c6c34f51187b52/Aula5/dados_vendas.csv"
df = pd.read_csv(url, encoding='ISO-8859-1', sep=';')

print("Dimensões do dataset:", df.shape)
print("\nTipos de dados:\n", df.dtypes)
print("\nValores ausentes:\n", df.isnull().sum())
print("\nResumo estatístico:\n", df.describe(include='all'))

# ==================================================
# ETAPA 2: TRANSFORMAÇÃO E TRATAMENTO DOS DADOS
# ==================================================

# Converter as datas para o formato padrão (YYYY-MM-DD)
date_columns = ['data_pedido', 'data_cadastro_produto', 'data_cadastro_cliente', 'data_inauguracao']
for col in date_columns:
    df[col] = pd.to_datetime(df[col], format='%d/%m/%Y', errors='coerce')

df['nome_fornecedor'] = df['nome_fornecedor'].fillna('Não identificado')
df['valor_desconto'] = df['valor_desconto'].fillna(df['valor_desconto'].mean())
df['preco_unitario'] = df.groupby('id_produto')['preco_unitario'].transform(lambda x: x.fillna(x.mean()))
df['quantidade'] = df['quantidade'].fillna(round(df['quantidade'].mean()))
df['quantidade'] = df['quantidade'].abs()
df['categoria_produto'] = df['categoria_produto'].str.capitalize()
df = df.drop_duplicates()

# Criar coluna concatenada para hierarquia: categoria > subcategoria
df['hierarquia_cat_subcat'] = df['categoria_produto'] + ' > ' + df['subcategoria_produto']

# Criaçao dos campos calculados
df['valor_bruto'] = df['preco_unitario'] * df['quantidade']
df['valor_liquido'] = df['valor_bruto'] - df['valor_desconto']
df['margem_de_lucro'] = ((df['preco_unitario'] - df['preco_custo']) / df['preco_unitario']) * 100
df['dias_desde_o_pedido'] = (pd.Timestamp.today() - df['data_pedido']).dt.days

# ==================================================
# ETAPA 3: CONSTRUÇÃO DAS DIMENSÕES E DA TABELA FATO
# ==================================================

# Dimensão Produto (Dim_Produto)
dim_produto_cols = ['id_produto', 'nome_produto', 'categoria_produto', 'subcategoria_produto',
                    'hierarquia_cat_subcat', 'preco_custo', 'preco_venda', 'peso_kg', 'fornecedor_id',
                    'nome_fornecedor', 'data_cadastro_produto']
dim_produto = df[dim_produto_cols].drop_duplicates().reset_index(drop=True)

# Dimensão Cliente (Dim_Cliente)
dim_cliente_cols = ['id_cliente', 'nome_cliente', 'email_cliente', 'telefone_cliente',
                    'data_cadastro_cliente', 'segmento_cliente', 'cidade_cliente', 'estado_cliente']
dim_cliente = df[dim_cliente_cols].drop_duplicates().reset_index(drop=True)

# Dimensão Loja (Dim_Loja)
dim_loja_cols = ['id_loja', 'nome_loja', 'endereco_loja', 'cidade_loja',
                 'estado_loja', 'regiao_loja', 'gerente_loja', 'data_inauguracao', 'tipo_loja']
dim_loja = df[dim_loja_cols].rename(columns={'data_inauguracao': 'data_inauguracao_loja'})\
                         .drop_duplicates().reset_index(drop=True)

# Dimensão Tempo (Dim_Tempo) a partir de 'data_pedido'
dim_tempo = df[['data_pedido']].drop_duplicates().dropna().reset_index(drop=True)
dim_tempo['id_tempo'] = range(1, len(dim_tempo) + 1)
dim_tempo['data'] = dim_tempo['data_pedido'].dt.strftime('%Y-%m-%d')
dim_tempo['ano'] = dim_tempo['data_pedido'].dt.year.astype(str)
dim_tempo['trimestre'] = dim_tempo['data_pedido'].dt.quarter
dim_tempo['mes'] = dim_tempo['data_pedido'].dt.month
dim_tempo['dia'] = dim_tempo['data_pedido'].dt.day
dim_tempo['dia_da_semana'] = dim_tempo['data_pedido'].dt.dayofweek
dim_tempo['fim_de_semana'] = dim_tempo['dia_da_semana'].apply(lambda x: 1 if x >= 5 else 0)
dim_tempo['periodo'] = dim_tempo['data_pedido'].dt.day.apply(lambda x: 'Manhã' if x % 3 == 0
                                                              else ('Tarde' if x % 3 == 1 else 'Noite'))

# Criar mapeamento de datas para IDs de tempo
data_to_id = dict(zip(dim_tempo['data_pedido'].dt.strftime('%Y-%m-%d'), dim_tempo['id_tempo']))

# Selecionar apenas as colunas necessárias para a dimensão tempo
dim_tempo = dim_tempo[['id_tempo', 'data', 'ano', 'trimestre', 'mes', 'dia', 'dia_da_semana', 'fim_de_semana', 'periodo']]

# Criar a Tabela Fato de Vendas (Fato_Vendas)
fato_vendas = df[['id_venda', 'id_pedido', 'id_produto', 'id_cliente', 'id_loja', 'quantidade',
                  'valor_bruto', 'valor_liquido', 'margem_de_lucro', 'dias_desde_o_pedido']].copy()

# Mapear as datas na tabela fato para os IDs de tempo corretos
fato_vendas['id_tempo'] = df['data_pedido'].dt.strftime('%Y-%m-%d').map(data_to_id)

# Remover registros com id_tempo nulo (datas que não existem na dimensão tempo)
fato_vendas = fato_vendas.dropna(subset=['id_tempo'])

# Ajustar os tipos de dados
for col in fato_vendas.select_dtypes(include=['int64', 'float64']).columns:
    fato_vendas[col] = fato_vendas[col].astype('float64')

# Dimensão Geografia (Dim_Geografia)
dim_geografia = df[['cidade_loja', 'estado_loja', 'regiao_loja']].drop_duplicates().reset_index(drop=True)
dim_geografia['id_geografia'] = range(1, len(dim_geografia) + 1)

# Criar hierarquia regiao > estado > cidade
dim_geografia['hierarquia_reg_est_cid'] = dim_geografia.apply(
    lambda x: f"{x['regiao_loja']} > {x['estado_loja']} > {x['cidade_loja']}",
    axis=1
)

# Reordenar as colunas e adicionar a hierarquia
dim_geografia = dim_geografia[['id_geografia', 'regiao_loja', 'estado_loja','cidade_loja', 'hierarquia_reg_est_cid']]

# Renomear as colunas para remover o sufixo _loja
dim_geografia = dim_geografia.rename(columns={
    'cidade_loja': 'cidade',
    'estado_loja': 'estado',
    'regiao_loja': 'regiao'
})

# Criar mapeamento de geografia para IDs 
geo_to_id = dict(zip(
    dim_geografia.apply(lambda x: f"{x['cidade']}|{x['estado']}|{x['regiao']}", axis=1),
    dim_geografia['id_geografia']
))

# Mapear as lojas para os IDs de geografia
fato_vendas['id_geografia'] = df.apply(
    lambda x: geo_to_id.get(f"{x['cidade_loja']}|{x['estado_loja']}|{x['regiao_loja']}"),
    axis=1
)

# ==================================================
# ETAPA 4: CRIAÇÃO DO DATA WAREHOUSE NO POSTGRESQL E CARGA DOS DADOS
# ==================================================

# Conexão ao PostgreSQL com as credenciais do banco de dados no site render
try:
    conn = psycopg2.connect(
        host="dpg-cvo8bn3uibrs73blqqpg-a.oregon-postgres.render.com",
        user="dados_venda_user",
        password="WnWOMu47eGyYl5nzacnnEdCjyJACS3TK",
        database="dados_venda"
    )
    # caso consiga conectar, printa a mensagem
    print("Conexão bem-sucedida!")
    conn.autocommit = True
    cursor = conn.cursor()
except Exception as e:
    # caso não consiga conectar, fecha o programa
    print("Erro na conexão:", e)
    exit()

# Criação das tabelas em PostgreSQL

# Limpar todas as tabelas existentes para não haver conflito
cursor.execute("DROP TABLE IF EXISTS Fato_Vendas CASCADE;")
cursor.execute("DROP TABLE IF EXISTS Dim_Produto CASCADE;")
cursor.execute("DROP TABLE IF EXISTS Dim_Cliente CASCADE;")
cursor.execute("DROP TABLE IF EXISTS Dim_Loja CASCADE;")
cursor.execute("DROP TABLE IF EXISTS Dim_Tempo CASCADE;")

# Dimensão Produto
cursor.execute("""
    CREATE TABLE IF NOT EXISTS Dim_Produto(
        id_produto numeric PRIMARY KEY,
        nome_produto varchar(100),
        categoria_produto varchar(50),
        subcategoria_produto varchar(50),
        hierarquia_cat_subcat varchar(150),
        preco_custo decimal(7,2),
        preco_venda decimal(7,2),
        peso_kg decimal(6,2),
        fornecedor_id numeric,
        nome_fornecedor varchar(60),
        data_cadastro_produto date
    );
""")

# Dimensão Cliente
cursor.execute("""
    CREATE TABLE IF NOT EXISTS Dim_Cliente(
        id_cliente numeric PRIMARY KEY,
        nome_cliente varchar(100),
        email_cliente varchar(150),
        telefone_cliente varchar(20),
        data_cadastro_cliente date,
        segmento_cliente varchar(50),
        cidade_cliente varchar(80),
        estado_cliente varchar(25)
    );
""")

# Dimensão Loja
cursor.execute("""
    CREATE TABLE IF NOT EXISTS Dim_Loja(
        id_loja numeric PRIMARY KEY,
        nome_loja varchar(100),
        endereco_loja varchar(250),
        cidade_loja varchar(80),
        estado_loja varchar(25),
        regiao_loja varchar(15),
        gerente_loja varchar(50),
        data_inauguracao_loja date,
        tipo_loja varchar(30)
    );
""")

# Dimensão Geografia
cursor.execute("""
    DROP TABLE IF EXISTS Dim_Geografia;
    CREATE TABLE Dim_Geografia (
        id_geografia NUMERIC PRIMARY KEY,
        cidade VARCHAR(100),
        estado VARCHAR(2),
        regiao VARCHAR(50),
        hierarquia_reg_est_cid VARCHAR(200),
        CONSTRAINT unique_geografia UNIQUE (regiao, estado, cidade)
    );
""")

# Dimensão Tempo
cursor.execute("""
    DROP TABLE IF EXISTS Dim_Tempo;
    CREATE TABLE Dim_Tempo (
        id_tempo NUMERIC PRIMARY KEY,
        data DATE,
        ano VARCHAR(4),
        trimestre INTEGER,
        mes INTEGER,
        dia INTEGER,
        dia_da_semana INTEGER,
        fim_de_semana INTEGER,
        periodo VARCHAR(10)
    );
""")

# Tabela Fato de Vendas
cursor.execute("""
    DROP TABLE IF EXISTS Fato_Vendas;
    CREATE TABLE Fato_Vendas (
        id_venda NUMERIC PRIMARY KEY,
        id_pedido NUMERIC,
        id_produto NUMERIC,
        id_cliente NUMERIC,
        id_loja NUMERIC,
        id_tempo NUMERIC,
        id_geografia NUMERIC,
        quantidade NUMERIC(10,2),
        valor_bruto NUMERIC(15,2),
        valor_liquido NUMERIC(15,2),
        margem_de_lucro NUMERIC(10,2),
        dias_desde_o_pedido NUMERIC(10,2),
        FOREIGN KEY (id_produto) REFERENCES Dim_Produto(id_produto),
        FOREIGN KEY (id_cliente) REFERENCES Dim_Cliente(id_cliente),
        FOREIGN KEY (id_loja) REFERENCES Dim_Loja(id_loja),
        FOREIGN KEY (id_tempo) REFERENCES Dim_Tempo(id_tempo),
        FOREIGN KEY (id_geografia) REFERENCES Dim_Geografia(id_geografia)
    );
""")

# Função utilitária para inserir dados usando execute_values
def insert_dataframe(cursor, table, df, columns):
    tuples = list(df[columns].itertuples(index=False, name=None))
    cols = ", ".join(columns)
    query = f"INSERT INTO {table} ({cols}) VALUES %s ON CONFLICT DO NOTHING;"
    psycopg2.extras.execute_values(cursor, query, tuples)

# Verificar e ajustar os tipos de dados antes da inserção
for col in fato_vendas.select_dtypes(include=['int64', 'float64']).columns:
    fato_vendas[col] = fato_vendas[col].astype('float64')

# Inserir dados nas tabelas e printa o número de registros inseridos
insert_dataframe(cursor, "Dim_Geografia", dim_geografia, dim_geografia.columns.tolist())
print("\nDim_Geografia carregada. Registros:", len(dim_geografia))

insert_dataframe(cursor, "Dim_Produto", dim_produto, dim_produto.columns.tolist())
print("\nDim_Produto carregada. Registros:", len(dim_produto))

insert_dataframe(cursor, "Dim_Cliente", dim_cliente, dim_cliente.columns.tolist())
print("Dim_Cliente carregada. Registros:", len(dim_cliente))

insert_dataframe(cursor, "Dim_Loja", dim_loja, dim_loja.columns.tolist())
print("Dim_Loja carregada. Registros:", len(dim_loja))

insert_dataframe(cursor, "Dim_Tempo", dim_tempo, dim_tempo.columns.tolist())
print("Dim_Tempo carregada. Registros:", len(dim_tempo))

# Converter para string antes da inserção para evitar problemas de tipo
fato_vendas_copy = fato_vendas.copy()
for col in fato_vendas_copy.columns:
    fato_vendas_copy[col] = fato_vendas_copy[col].astype(str)

insert_dataframe(cursor, "Fato_Vendas", fato_vendas_copy, fato_vendas_copy.columns.tolist())
print("Fato_Vendas carregada. Registros:", len(fato_vendas_copy))

cursor.close()
conn.close()

print("\nPipeline completa. Dados carregados no banco PostgreSQL.")
