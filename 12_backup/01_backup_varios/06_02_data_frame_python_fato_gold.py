# ============================================================
# SCRIPT 2 — CRIAÇÃO DO GOLD FATO
# ============================================================

import pandas as pd
from pathlib import Path

BASE = Path(r"G:\Meu Drive\mini-airflow")

# 1. Carrega o silver business
silver_business = BASE / "05_business_silver" / "tb_df_py.parquet"
df = pd.read_parquet(silver_business)

# 2. Carrega a dimensão classe (onde está o fator_classe)
dim_classe = pd.read_parquet(BASE / "06_analitico_gold" / "dim_classe_processamento.parquet")

# 3. JOIN correto: tp_classe (silver) ↔ id_classe (dimensão)
df = df.merge(dim_classe, left_on="tp_classe", right_on="id_classe", how="left")

# 4. Calcula a métrica GOLD
df["valor_ponderado"] = df["nr_valor"] * df["fator_classe"]

# 5. Salva o GOLD FATO (sempre sobrescreve)
PASTA_GOLD = BASE / "06_analitico_gold"
df.to_parquet(PASTA_GOLD / "tb_fato.parquet", index=False)

print("GOLD FATO gerado com sucesso:", PASTA_GOLD / "tb_fato.parquet")