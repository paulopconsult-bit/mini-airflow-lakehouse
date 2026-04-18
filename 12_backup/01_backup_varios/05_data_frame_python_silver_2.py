# ============================================================
# SCRIPT 05: SILVER BUSINESS (data_frame_python)
# ------------------------------------------------------------
# Lê o Silver Transacional, remove caracteres inválidos,
# reconverte tipos, aplica regras de negócio principais, cria colunas
# derivadas e elimina duplicados. 
# Gera o parquet final do Silver Business para consumo analítico.
#
# ============================================================
# 0. IMPORTS
# ============================================================
import os
import pandas as pd

# ============================================================
# 1. BASE DO MINI-LAKEHOUSE (CAMINHO ABSOLUTO)
# ============================================================
BASE = r"G:\Meu Drive\mini-airflow"

PASTA_SILVER_TRANS = os.path.join(BASE, "04_curadoria_silver")
PASTA_SILVER_BUS   = os.path.join(BASE, "05_business_silver")

os.makedirs(PASTA_SILVER_BUS, exist_ok=True)

ARQUIVO_TRANS = os.path.join(PASTA_SILVER_TRANS, "tb_df_py.parquet")
ARQUIVO_BUS   = os.path.join(PASTA_SILVER_BUS, "tb_df_py.parquet")

# ============================================================
# 2. LEITURA DO SILVER TRANSACIONAL
# ============================================================
df = pd.read_parquet(ARQUIVO_TRANS)

# ============================================================
# 3. LIMPEZA PROFUNDA DE CARACTERES (TODAS AS COLUNAS)
# ============================================================

caracteres_para_remover = ['"', '\\']

# 3.1 Converte tudo para string temporariamente
for col in df.columns:
    df[col] = df[col].astype(str)

# 3.2 Remove caracteres problemáticos
for col in df.columns:
    for c in caracteres_para_remover:
        df[col] = df[col].replace(c, "", regex=False)

# ============================================================
# 4. RECONVERSÃO PARA TIPOS CORRETOS (SEMÂNTICA)
# ============================================================

# IDs devem ser strings
df["id_registro"] = df["id_registro"].astype(str)
df["id_pessoa"]   = df["id_pessoa"].astype(str)
df["id_produto"]  = df["id_produto"].astype(str)

# tp_classe é categórico numérico
df["tp_classe"] = pd.to_numeric(df["tp_classe"], errors="coerce")

# nr_valor é numérico contínuo
df["nr_valor"] = pd.to_numeric(df["nr_valor"], errors="coerce")

# dt_criacao é datetime
df["dt_criacao"] = pd.to_datetime(df["dt_criacao"], errors="coerce")

# ============================================================
# 5. LIMPEZA DE STRINGS (APENAS COLUNAS TEXTUAIS)
# ============================================================
colunas_string = ["id_registro"]

for col in colunas_string:
    df[col] = df[col].str.strip()

# ============================================================
# 6. REGRAS DE NEGÓCIO (EXEMPLOS)
# ============================================================

# Remove valores negativos
df = df[df["nr_valor"] >= 0]

# Cria colunas de ano e mês
df["ano"] = df["dt_criacao"].dt.year
df["mes"] = df["dt_criacao"].dt.month

# ============================================================
# 7. REMOÇÃO DE DUPLICADOS
# ============================================================
df = df.drop_duplicates(subset=["id_registro"])

# ============================================================
# 8. SALVA O SILVER BUSINESS
# ============================================================
df.to_parquet(ARQUIVO_BUS, index=False)

print("Silver Business gerado com sucesso:", ARQUIVO_BUS)