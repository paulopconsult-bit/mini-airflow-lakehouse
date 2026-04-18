# ============================================================
# SCRIPT SILVER_1 TRANSACIONAL (data_frame_python)
# ------------------------------------------------------------
# Lê o parquet do Bronze, renomeia colunas conforme padrão
# do Silver, aplica padronização snake_case e salva como
# tb_df_py.parquet na camada Silver Transacional (curadoria).
# Não altera valores nem aplica regras de negócio.
#
# -----------------------------------------------------------
# SCRIPT 03 - SILVER TRANSACIONAL (04_curadoria_silver)
# -----------------------------------------------------------
# Objetivo:
#   - Padronizar estrutura sem alterar valores
#   - Renomear colunas para snake_case
#   - Ajustar nomes específicos conforme regra definida
#   - Salvar arquivo padronizado como tb_df_py.parquet
#
# O que NÃO faz:
#   - Não altera valores
#   - Não converte tipos
#   - Não aplica regra de negócio
#   - Não trata nulos
#   - Não remove duplicatas
#   - Não cria colunas novas
#
# Entrada:
#   03_processado_bronze/data_frame_python.parquet
#
# Saída:
#   04_curadoria_silver/tb_df_py.parquet
# -----------------------------------------------------------

import os
import pandas as pd

# Caminhos das pastas
BASE = r"G:\Meu Drive\mini-airflow"

PASTA_BRONZE = os.path.join(BASE, "03_processado_bronze")
PASTA_SILVER = os.path.join(BASE, "04_curadoria_silver")

# Arquivo de entrada e saída
ARQUIVO_BRONZE = os.path.join(PASTA_BRONZE, "data_frame_python.parquet")
ARQUIVO_SILVER = os.path.join(PASTA_SILVER, "tb_df_py.parquet")

# Garantir que a pasta silver exista
os.makedirs(PASTA_SILVER, exist_ok=True)

# -----------------------------------------------------------
# 1. Ler o arquivo do Bronze
# -----------------------------------------------------------
df = pd.read_parquet(ARQUIVO_BRONZE)

# -----------------------------------------------------------
# 2. Renomear colunas conforme regras do Silver Transacional
# -----------------------------------------------------------
# Apenas 3 colunas serão renomeadas:
# classe     -> tp_classe
# valor      -> nr_valor
# gerado_em  -> dt_criacao

rename_dict = {
    "classe": "tp_classe",
    "valor": "nr_valor",
    "gerado_em": "dt_criacao"
}

df = df.rename(columns=rename_dict)

# -----------------------------------------------------------
# 3. Padronizar snake_case (garantia)
# -----------------------------------------------------------
df.columns = (
    df.columns
    .str.lower()
    .str.replace(" ", "_")
    .str.replace("-", "_")
    .str.normalize("NFKD")
    .str.encode("ascii", errors="ignore")
    .str.decode("utf-8")
)

# -----------------------------------------------------------
# 4. Salvar o arquivo no Silver Transacional
# -----------------------------------------------------------
df.to_parquet(ARQUIVO_SILVER, index=False)

print("Silver Transacional gerado com sucesso:", ARQUIVO_SILVER)