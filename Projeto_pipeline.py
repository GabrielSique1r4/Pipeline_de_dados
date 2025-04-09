import pandas as pd
import psycopg2
from datetime import datetime

# Carregar o CSV diretamente do GitHub.
url = "https://raw.githubusercontent.com/GabrielSique1r4/Pipeline_de_dados/e34d31a12991a44f58a1aa5210c6c34f51187b52/Aula5/dados_vendas.csv"
df = pd.read_csv(url, encoding='ISO-8859-1', sep=';')

# Pré-processamento
df['data_pedido'] = pd.to_datetime(df['data_pedido'], format='%d/%m/%Y', errors='coerce')
df['data_cadastro_produto'] = pd.to_datetime(df['data_cadastro_produto'], format='%d/%m/%Y', errors='coerce')
df['data_cadastro_cliente'] = pd.to_datetime(df['data_cadastro_cliente'], format='%d/%m/%Y', errors='coerce')
df['data_inauguracao'] = pd.to_datetime(df['data_inauguracao'], format='%d/%m/%Y', errors='coerce')

# Tratamento dos Dados
df.nome_fornecedor = df.nome_fornecedor.fillna('Não identificado')
df.valor_desconto = df.valor_desconto.transform(lambda x: x.fillna(x.mean()))
df.preco_unitario = df.groupby('id_produto')['preco_unitario'].transform(lambda x: x.fillna(x.mean()))
df.quantidade = df.quantidade.transform(lambda x: x.fillna(round(x.mean())))
df['quantidade'] = df['quantidade'].abs()
df['categoria_produto'] = df['categoria_produto'].str.capitalize()

# Campos Calculados
df['valor_bruto'] = df.preco_unitario * df.quantidade
df['valor_liquido'] = df.valor_bruto - df.valor_desconto
df['margem_de_lucro'] = (((df.preco_unitario - df.preco_custo) / df.preco_unitario) * 100)
df['dias_desde_o_pedido'] = (pd.Timestamp.today() - df.data_pedido).dt.days

# Conexão ao PostgreSQL
conn = psycopg2.connect(
    host="dpg-cvo8bn3uibrs73blqqpg-a.oregon-postgres.render.com",
    user="dados_venda_user",
    password="WnWOMu47eGyYl5nzacnnEdCjyJACS3TK",
    database="dados_venda"
)
conn.autocommit = True
cursor = conn.cursor()

# Criação das tabelas
cursor.execute("""
CREATE TABLE IF NOT EXISTS cliente(
    id_cliente int PRIMARY KEY,
    nome_cliente varchar(50),
    email_cliente varchar(150),
    telefone_cliente varchar(20),
    data_cadastro_cliente date,
    segmento_cliente varchar(25),
    cidade_cliente varchar(80),
    estado_cliente varchar(25)
);

CREATE TABLE IF NOT EXISTS loja(
    id_loja int PRIMARY KEY,
    nome_loja varchar(100),
    endereco_loja varchar(250),
    cidade_loja varchar(80),
    estado_loja varchar(25),
    regiao_loja varchar(15),
    gerente_loja varchar(50),
    data_inauguracao_loja date,
    tipo_loja varchar(30)
);

CREATE TABLE IF NOT EXISTS produto(
    id_produto int PRIMARY KEY,
    nome_produto varchar(100),
    categoria_produto varchar(50),
    subcategoria_produto varchar(50),
    preco_custo decimal(7,2),
    preco_venda decimal(7,2),
    peso_kg decimal(6,2),
    fornecedor_id int,
    nome_fornecedor varchar(60),
    data_cadastro_produto date,
    em_estoque boolean,
    margem_lucro decimal(6,2)
);

CREATE TABLE IF NOT EXISTS pedido(
    id_pedido int PRIMARY KEY,
    data_pedido date,
    quantidade decimal(4,1),
    preco_unitario decimal(7,2),
    valor_desconto decimal(7,2),
    custo_frete decimal(7,2),
    status_pedido varchar(25),
    metodo_pagamento varchar(30),
    nota_fiscal varchar(20),
    dias_desde_pedido int,
    valor_bruto decimal(8,2),
    valor_liquido decimal(8,2)
);

CREATE TABLE IF NOT EXISTS produto_pedido(
    id_produto_pedido serial PRIMARY KEY,
    id_pedido INT REFERENCES pedido(id_pedido),
    id_produto INT REFERENCES produto(id_produto)
);

CREATE TABLE IF NOT EXISTS venda(
    id_venda int PRIMARY KEY,
    id_cliente int REFERENCES cliente(id_cliente),
    id_pedido int REFERENCES pedido(id_pedido),
    id_loja int REFERENCES loja(id_loja)
);
""")

# Fechar conexão
cursor.close()
conn.close()
