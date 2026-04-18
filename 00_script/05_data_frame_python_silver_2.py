# ============================================================
# SCRIPT 05: SILVER BUSINESS (ATENÇÃO: DATA CAMPO DT_CRIACAO)
# ============================================================
import os
import pandas as pd

BASE = r"G:\Meu Drive\mini-airflow"
PASTA_SILVER_TRANS = os.path.join(BASE, "04_curadoria_silver")
PASTA_SILVER_BUS   = os.path.join(BASE, "05_business_silver")
os.makedirs(PASTA_SILVER_BUS, exist_ok=True)

ARQUIVO_TRANS = os.path.join(PASTA_SILVER_TRANS, "tb_df_py.parquet")
ARQUIVO_BUS   = os.path.join(PASTA_SILVER_BUS, "tb_df_py.parquet")

# 2. LEITURA
df = pd.read_parquet(ARQUIVO_TRANS)

# ============================================================
# 3. LIMPEZA PROFUNDA (COM PROTEÇÃO PARA DATA)
# ============================================================
caracteres_para_remover = ['"', '\\']

for col in df.columns:
    if col != 'dt_criacao': # PROTEGE A DATA
        df[col] = df[col].astype(str)
        for c in caracteres_para_remover:
            df[col] = df[col].replace(c, "", regex=False)

# ============================================================
# 4. RECONVERSÃO E SEMÂNTICA (AQUI DEFINIMOS OS TIPOS)
# ============================================================

# IDs e Categorias -> STRING
df["id_registro"] = df["id_registro"].astype(str)
df["id_pessoa"]   = df["id_pessoa"].astype(str)
df["id_produto"]  = df["id_produto"].astype(str)
df["tp_classe"]   = df["tp_classe"].astype(str) # <--- MUDANÇA SOLICITADA

# Métricas Financeiras -> NUMÉRICO
df["nr_valor"] = pd.to_numeric(df["nr_valor"], errors="coerce")

# Tempo -> DATETIME
df["dt_criacao"] = pd.to_datetime(df["dt_criacao"], errors="coerce")

# ============================================================
# 5. REGRAS DE NEGÓCIO E DERIVAÇÕES
# ============================================================
df = df[df["nr_valor"] >= 0]
df["ano"] = df["dt_criacao"].dt.year
df["mes"] = df["dt_criacao"].dt.month
df = df.drop_duplicates(subset=["id_registro"])

# ============================================================
# 8. SALVAMENTO PROFISSIONAL (PYARROW)
# ============================================================
df.to_parquet(
    ARQUIVO_BUS, 
    index=False,
    engine='pyarrow',
    coerce_timestamps='us', # Fundamental para o BigQuery reconhecer a data
    allow_truncated_timestamps=True
)

print("Silver Business gerado com sucesso (Data e tp_classe ajustados):", ARQUIVO_BUS)