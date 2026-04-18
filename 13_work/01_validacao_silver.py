# ============================================================
# SCRIPT VALIDAÇÃO E APOIO DO SILVER TRANSACIONAL (data_frame_python)
# Não orquestrado, serve para inspeção e validação dos dados do Silver Transacional.
# ------------------------------------------------------------
# Lê o arquivo do Silver Transacional e realiza inspeções
# completas: info, tipos, nulos, estatísticas, agregações, amostras e
# frequências por coluna. Serve para validar a qualidade
# antes de avançar para o Silver Business. 
#
# ============================================================

import os
import pandas as pd

# Caminho base do mini-lakehouse
BASE = r"G:\Meu Drive\mini-airflow"

# Caminho do Silver Transacional
PASTA_SILVER_TRANS = os.path.join(BASE, "04_curadoria_silver")
ARQUIVO_TRANS = os.path.join(PASTA_SILVER_TRANS, "tb_df_py.parquet")

# Leitura do arquivo
df = pd.read_parquet(ARQUIVO_TRANS)

# ============================================================
# 1. INSPEÇÃO GERAL
# ============================================================

print("\n=== INFO ===")
print(df.info())

print("\n=== TIPOS ===")
print(df.dtypes)

print("\n=== NULOS ===")
print(df.isna().sum())

print("\n=== DESCRIBE ===")
print(df.describe(include="all"))

print("\n=== SAMPLE (5 linhas aleatórias) ===")
print(df.sample(5))

print("\n=== MEMÓRIA ===")
print(df.memory_usage(deep=True))

# ============================================================
# 2. AGRUPADORES PARA TODAS AS COLUNAS
# ============================================================

print("\n=== AGRUPADORES (VALORES ÚNICOS E FREQUÊNCIAS) ===")

for col in df.columns:
    print(f"\n--- {col} ---")
    
    # Frequências (limitado a 50 para não poluir o console)
    print(df[col].value_counts(dropna=False).head(50))
    
    # Quantidade total de valores únicos
    print(f"Total de valores únicos: {df[col].nunique()}")