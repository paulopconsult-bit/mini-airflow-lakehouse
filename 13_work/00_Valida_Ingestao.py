import pandas as pd

caminho = r"G:\Meu Drive\mini-airflow\03_processado_bronze\manifesto_ingestao.csv"

df = pd.read_csv(caminho, sep=";", engine="python")

qtd_registros = len(df)
soma_quantidade = df["quantidade_linhas"].sum()

print("Quantidade de registros:", qtd_registros)
print("Soma da coluna quantidade_linhas:", soma_quantidade)