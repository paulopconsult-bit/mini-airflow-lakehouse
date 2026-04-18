# ============================================================
# SCRIPT 06_02 — CRIAÇÃO DO GOLD FATO
# ============================================================
import pandas as pd
from pathlib import Path

BASE = Path(r"G:\Meu Drive\mini-airflow")

# 1. Carrega o silver business
silver_business = BASE / "05_business_silver" / "tb_df_py.parquet"
df = pd.read_parquet(silver_business)

# 2. Carrega a dimensão classe
dim_classe = pd.read_parquet(BASE / "06_analitico_gold" / "dim_classe_processamento.parquet")

# 3. GARANTIA: Converte os campos de JOIN para STRING para não dar erro
df["tp_classe"] = df["tp_classe"].astype(str)
dim_classe["id_classe"] = dim_classe["id_classe"].astype(str)

# 4. JOIN (tp_classe ↔ id_classe)
df = df.merge(dim_classe, left_on="tp_classe", right_on="id_classe", how="left")

# 5. Métrica GOLD
df["valor_ponderado"] = df["nr_valor"] * df["fator_classe"]

# 6. SALVAMENTO PADRÃO OURO PARA NUVEM
PASTA_GOLD = BASE / "06_analitico_gold"
df.to_parquet(
    PASTA_GOLD / "tb_fato.parquet", 
    index=False,
    engine='pyarrow',
    coerce_timestamps='us',
    allow_truncated_timestamps=True
)

print("GOLD FATO gerado com sucesso e formatado para o BigQuery!")