# ============================================================
# SCRIPT MOVIMENTAÇÃO E BRONZE (data_frame_python)
# ------------------------------------------------------------
# Move arquivos RAW estáveis para PROCESSANDO, lê os CSVs,
# consolida tudo em um único parquet BRONZE e arquiva os CSVs também no bronze.
# Remove arquivos residuais da pasta PROCESSANDO e da pasta RAW.
# Esta etapa organiza e padroniza os dados antes do Silver.
# 
# ============================================================
# 1. BIBLIOTECAS UTILIZADAS
# ============================================================

import os
import shutil
import pandas as pd


# ============================================================
# 2. CONFIGURAÇÃO DAS PASTAS DO MINI-LAKEHOUSE
# ============================================================

BASE = r"G:\Meu Drive\mini-airflow"

PASTA_RAW         = os.path.join(BASE, "01_entrada_raw")
PASTA_PROCESSANDO = os.path.join(BASE, "02_processando")
PASTA_BRONZE      = os.path.join(BASE, "03_processado_bronze")

ARQUIVO_BRONZE = os.path.join(PASTA_BRONZE, "data_frame_python.parquet")

# ============================================================
# 3. VERIFICAÇÃO DA EXISTÊNCIA DAS PASTAS
# ============================================================

for pasta in [PASTA_RAW, PASTA_PROCESSANDO, PASTA_BRONZE]:
    if not os.path.exists(pasta):
        print(f"ERRO: A pasta {pasta} não existe. O Script 01 deve criá-la.")
        exit()

print("\n=== INICIANDO SCRIPT 02 (INGESTÃO INTERNA) ===\n")


# ============================================================
# 4. LISTAR TODOS OS ARQUIVOS DA PASTA RAW
# ============================================================

todos_arquivos_raw = os.listdir(PASTA_RAW)
print(f"Arquivos encontrados no RAW: {todos_arquivos_raw}\n")


# ============================================================
# 5. FUNÇÃO PARA VERIFICAR SE O ARQUIVO ESTÁ FECHADO (SEM LOCK)
# ============================================================

def arquivo_estavel(caminho):
    try:
        with open(caminho, "rb+"):
            return True
    except PermissionError:
        return False


# ============================================================
# 6. FILTRAR, TESTAR LOCK E MOVER PARA PROCESSANDO
# ============================================================

arquivos_movidos = []

for arquivo in todos_arquivos_raw:

    if "data_frame_python" not in arquivo.lower():
        continue

    origem = os.path.join(PASTA_RAW, arquivo)

    if not arquivo_estavel(origem):
        print(f"Arquivo em uso (lock ativo), ignorado: {arquivo}")
        continue

    destino = os.path.join(PASTA_PROCESSANDO, arquivo)
    shutil.move(origem, destino)
    arquivos_movidos.append(arquivo)
    print(f"Movido para processamento: {arquivo}")

print()

if not arquivos_movidos:
    print("Nenhum arquivo pronto (sem lock) para processamento.")
    exit()


# ============================================================
# 7. PROCESSAR ARQUIVOS DA PASTA PROCESSANDO
# ============================================================

for arquivo in arquivos_movidos:

    caminho_processando = os.path.join(PASTA_PROCESSANDO, arquivo)
    print(f"Processando arquivo: {arquivo}")

    try:
        df = pd.read_csv(caminho_processando)
    except Exception as e:
        print(f"Erro ao ler {arquivo}: {e}")
        continue

    # Consolidação correta do parquet (sem append=True)
    try:
        if os.path.exists(ARQUIVO_BRONZE):
            bronze_df = pd.read_parquet(ARQUIVO_BRONZE)
            bronze_df = pd.concat([bronze_df, df], ignore_index=True)
            bronze_df.to_parquet(ARQUIVO_BRONZE, index=False)
        else:
            df.to_parquet(ARQUIVO_BRONZE, index=False)
    except Exception as e:
        print(f"Erro ao escrever no BRONZE: {e}")
        continue

    # Mover CSV para histórico
    destino_hist = os.path.join(PASTA_BRONZE, arquivo)
    shutil.move(caminho_processando, destino_hist)
    print(f"Arquivo movido para histórico: {arquivo}\n")


# ============================================================
# 8. LIMPEZA FINAL DA PASTA PROCESSANDO
# ============================================================

for arquivo in os.listdir(PASTA_PROCESSANDO):
    caminho = os.path.join(PASTA_PROCESSANDO, arquivo)
    os.remove(caminho)
    print(f"Arquivo residual removido da pasta PROCESSANDO: {arquivo}")

print("\n=== PROCESSO FINALIZADO COM SUCESSO ===\n")