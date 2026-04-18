# ============================================================
# Criando Arquivos e Pastas com Python Usando os.makedirs()
# ------------------------------------------------------------
# Este exemplo mostra como criar uma nova pasta no sistema de
# arquivos utilizando a função os.makedirs(). 
#
# O parâmetro exist_ok=True garante que o script não gere erro
# caso a pasta já exista (não cria nada), tornando o processo seguro e repetível.
# ============================================================

# import os

# caminho_novo = r"G:\Meu Drive\mini-airflow\05_analitico_gold"
# os.makedirs(caminho_novo, exist_ok=True)

# print("Comando para criar pasta executado com sucesso!")

# ============================================================
# RENOMEANDO Arquivos e Pastas no Sistema de Arquivos com Python
# ------------------------------------------------------------
# Este exemplo demonstra como alterar o nome de uma pasta
# utilizando a função os.rename(). 
# Se o caminho existir e você tiver permissão de escrita, 
# a pasta será renomeada com sucesso.
# ============================================================

# import os

# nome_antigo = r"G:\Meu Drive\mini-airflow\04_erros"
# nome_novo   = r"G:\Meu Drive\mini-airflow\06_erros"

# os.rename(nome_antigo, nome_novo)

# print("Pasta ou arquivo renomeado com sucesso!")

# ============================================================
# EXCLUINDO uma pasta vazia com os.rmdir()
# ------------------------------------------------------------
# Este exemplo mostra como remover uma pasta utilizando a
# função os.rmdir(). Essa função só funciona se a pasta estiver
# completamente vazia. Caso exista qualquer arquivo ou subpasta
# dentro dela, o Python gerará um erro.
#
# Use este método quando quiser remover diretórios criados
# incorretamente ou limpar estruturas vazias.
# ============================================================

# import os

# caminho_pasta = r"G:\Meu Drive\mini-airflow\10_entrada"

# os.rmdir(caminho_pasta)

# print("Pasta excluída com sucesso!")

# ============================================================
# Movendo um arquivo entre diretórios com shutil.move()
# ------------------------------------------------------------
# A função shutil.move() funciona como um "recortar e colar".
# Ela move arquivos ou pastas para um novo diretório.
# Se o destino tiver o mesmo nome, o item será sobrescrito.
# ============================================================

# import shutil

# origem = r"G:\Meu Drive\mini-airflow\00_setup_geral_manual.py"
# destino = r"G:\Meu Drive\mini-airflow\00_script\00_setup_geral_manual.py"

# shutil.move(origem, destino)

# print("Arquivo movido com sucesso!")

# ============================================================
# LER .Parquet e EXIBIR INFORMAÇÕES com Pandas
# ------------------------------------------------------------
# O formato Parquet é binário e colunar, ideal para Data Lakes.
# Ele armazena os dados de forma comprimida e otimizada, permitindo:
#   - leitura muito mais rápida que CSV
#   - economia de espaço em disco
#   - preservação dos tipos de dados (int, float, datetime etc.)
# ============================================================

import pandas as pd

# Lê o arquivo Parquet e carrega no DataFrame
df = pd.read_parquet(
    r"G:\Meu Drive\mini-airflow\05_business_silver\tb_df_py.parquet"
)

# Mostra algumas linhas para validar o conteúdo
print(df.head())

# Exibe estrutura: colunas, tipos e quantidade de registros
print(df.info())
