# ============================================================
# SCRIPT 02: MOVIMENTAÇÃO, BRONZE E MANIFESTO (CORRIGIDO)
# ------------------------------------------------------------
# 1. Faz Backup do original na pasta 12_backup.
# 2. Consolida os dados no Parquet Bronze com tratamento de data.
# 3. Registra a operação no 'manifesto_ingestao.csv'.
# 4. Deleta o CSV temporário para economizar espaço no Drive.
# ============================================================

import os
import shutil
import pandas as pd
from datetime import datetime

# ============================================================
# 2. CONFIGURAÇÃO DAS PASTAS DO MINI-LAKEHOUSE
# ============================================================
BASE = r"G:\Meu Drive\mini-airflow"

PASTA_RAW         = os.path.join(BASE, "01_entrada_raw")
PASTA_PROCESSANDO = os.path.join(BASE, "02_processando")
PASTA_BRONZE      = os.path.join(BASE, "03_processado_bronze")
# Caminho exato que você criou:
PASTA_BACKUP_RAW  = os.path.join(BASE, "12_backup", "00_backup_arquivos_originais")

ARQUIVO_BRONZE    = os.path.join(PASTA_BRONZE, "data_frame_python.parquet")
ARQUIVO_MANIFESTO = os.path.join(PASTA_BRONZE, "manifesto_ingestao.csv")

# Garante a existência de todas as pastas necessárias
for p in [PASTA_RAW, PASTA_PROCESSANDO, PASTA_BRONZE, PASTA_BACKUP_RAW]:
    os.makedirs(p, exist_ok=True)

print("\n=== INICIANDO SCRIPT 02 (INGESTÃO + BACKUP + MANIFESTO) ===\n")

# ============================================================
# 4. FUNÇÃO PARA TESTAR LOCK E LISTAR ARQUIVOS
# ============================================================
def arquivo_estavel(caminho):
    try:
        with open(caminho, "rb+"): return True
    except PermissionError: return False

todos_arquivos_raw = os.listdir(PASTA_RAW)
arquivos_movidos = []

# Filtragem e movimentação para a pasta 02_processando
for arquivo in todos_arquivos_raw:
    if "data_frame_python" not in arquivo.lower(): continue
    origem = os.path.join(PASTA_RAW, arquivo)
    
    if not arquivo_estavel(origem):
        print(f"Arquivo em uso, ignorado: {arquivo}")
        continue
    
    destino = os.path.join(PASTA_PROCESSANDO, arquivo)
    shutil.move(origem, destino)
    arquivos_movidos.append(arquivo)

if not arquivos_movidos:
    print("Nenhum arquivo pronto para processamento no momento.")
    exit()

# ============================================================
# 7. PROCESSAR, GERAR BACKUP E REGISTRAR MANIFESTO
# ============================================================
for arquivo in arquivos_movidos:
    caminho_processando = os.path.join(PASTA_PROCESSANDO, arquivo)
    print(f"Lendo: {arquivo}")

    # 7.1 BACKUP (Cópia fiel para a sua nova pasta 12_backup)
    try:
        shutil.copy(caminho_processando, os.path.join(PASTA_BACKUP_RAW, arquivo))
        print(f"  -> [OK] Backup salvo em 12_backup")
    except Exception as e:
        print(f"  -> [AVISO] Falha ao realizar backup: {e}")

    # 7.2 PROCESSAMENTO BRONZE (Tratamento de Data)
    try:
        # 'parse_dates' resolve a data vindo como texto no CSV
        df = pd.read_csv(caminho_processando, parse_dates=['gerado_em'])
        qtd_linhas = len(df)
        
        if os.path.exists(ARQUIVO_BRONZE):
            bronze_df = pd.read_parquet(ARQUIVO_BRONZE)
            # Garante que o Bronze atual também esteja como Datetime antes do concat
            bronze_df['gerado_em'] = pd.to_datetime(bronze_df['gerado_em'])
            df = pd.concat([bronze_df, df], ignore_index=True)
        
        # Salva consolidado na Bronze
        df.to_parquet(ARQUIVO_BRONZE, index=False)
        print(f"  -> [OK] Consolidado na camada Bronze")
            
        # 7.3 REGISTRO NO MANIFESTO (Auditabilidade)
        novo_registro = {
            "data_processamento": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "arquivo_origem": arquivo,
            "quantidade_linhas": qtd_linhas,
            "status": "SUCESSO"
        }
        
        log_df = pd.DataFrame([novo_registro])
        if not os.path.exists(ARQUIVO_MANIFESTO):
            log_df.to_csv(ARQUIVO_MANIFESTO, index=False, sep=";")
        else:
            log_df.to_csv(ARQUIVO_MANIFESTO, mode='a', header=False, index=False, sep=";")
        print(f"  -> [OK] Registrado no manifesto_ingestao.csv")
            
        # 7.4 LIMPEZA FINAL (Remove do processando para poupar espaço)
        os.remove(caminho_processando)

    except Exception as e:
        print(f"  -> [ERRO] Falha ao processar {arquivo}: {e}")

print("\n=== SCRIPT 02 FINALIZADO COM SUCESSO ===\n")