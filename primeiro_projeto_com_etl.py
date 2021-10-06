import sqlalchemy
import pandas as pd

#CRIA A ENGINE DE ORIGEM

engineorigem = sqlalchemy.create_engine('mssql+pyodbc://usuarioprd:123456@./PRIMEIRO_PROJETO?driver=SQL Server')

#IMPORTA AS TABELAS DO SQL SERVER

fato_pedidos = pd.read_sql(sql='SELECT * FROM fato_pedidos', con=engineorigem)
dim_produtos = pd.read_sql(sql='SELECT * FROM dim_produtos', con=engineorigem)
dim_lojas = pd.read_sql(sql='SELECT * FROM dim_lojas', con=engineorigem)

#REALIZA O TRATAMENTO DOS DADOS

fato_dw = pd.merge(left=fato_pedidos, right=dim_produtos, how='left', left_on='produto', right_on='id')
fato_dw = pd.merge(left=fato_dw, right=dim_lojas, how='left', left_on='loja', right_on='id')
fato_dw.drop(columns=['id_x', 'id_y', 'id', 'produto_x', 'loja'], inplace=True)
fato_dw = fato_dw[['produto_y', 'valor', 'data', 'estado', 'cidade', 'logradouro']]
fato_dw.rename(columns={'produto_y': 'Produto', 'valor': 'Valor', 'data': 'Data', 'estado': 'Estado', 'cidade': 'Cidade', 'logradouro': 'Logradouro'}, inplace=True)

#CRIA A ENGINE DE DESTINO

enginedestino = sqlalchemy.create_engine('postgresql+psycopg2://usuarioadmin:123456@localhost/PRIMEIRO_PROJETO')

#CALCULA O CHUNKSIZE MAXIMO

cs = 2097 // len(fato_dw.columns)

if cs > 1000:
    cs = 1000
else:
    cs = cs

#EXPORTA TABELA PARA O POSTGRES

fato_dw.to_sql(name='pedidos', con=enginedestino, if_exists='replace', index=False, chunksize=cs)




