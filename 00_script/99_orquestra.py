import subprocess
import os
import sys
from datetime import datetime

# ============================================================
# CONFIGURAÇÕES GERAIS
# ============================================================

BASE = r"G:\Meu Drive\mini-airflow\00_script"
ROOT = r"G:\Meu Drive\mini-airflow"

LOG_DIR = os.path.join(ROOT, "08_logs")
ERRO_DIR = os.path.join(ROOT, "07_erros")
VALIDACAO_DIR = os.path.join(ROOT, "09_validacao")

os.makedirs(LOG_DIR, exist_ok=True)
os.makedirs(ERRO_DIR, exist_ok=True)
os.makedirs(VALIDACAO_DIR, exist_ok=True)

log_file = os.path.join(LOG_DIR, f"execucao_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")


# ============================================================
# FUNÇÃO DE LOG
# ============================================================

def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    linha = f"[{timestamp}] {msg}"
    print(linha)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(linha + "\n")


# ============================================================
# SISTEMA DE TRAVA (LOCK)
# ============================================================

LOCK_FILE = os.path.join(BASE, "orquestra_em_execucao.lock")

if os.path.exists(LOCK_FILE):
    log("A orquestração já está em execução. Execução atual cancelada.")
    raise SystemExit()

with open(LOCK_FILE, "w", encoding="utf-8") as f:
    f.write("orquestracao em andamento")


# ============================================================
# IDENTIFICAR ORIGEM DA EXECUÇÃO
# ============================================================

try:
    usuario = os.getlogin()
except Exception:
    usuario = "desconhecido"

if sys.stdin.isatty():
    origem_execucao = f"Execução manual pelo usuário: {usuario}"
else:
    origem_execucao = f"Execução automática pelo Windows (usuário: {usuario})"

log(f"Origem da execução: {origem_execucao}")


# ============================================================
# EXECUTAR SCRIPT
# ============================================================

def executar(script: str):
    caminho = os.path.join(BASE, script)
    log(f"Iniciando: {script}")

    try:
        resultado = subprocess.run(
            ["python", caminho],
            capture_output=True,
            text=True
        )

        if resultado.stdout:
            log(f"STDOUT {script}: {resultado.stdout.strip()}")

        if resultado.stderr:
            log(f"STDERR {script}: {resultado.stderr.strip()}")

        if resultado.returncode != 0:
            erro_path = os.path.join(
                ERRO_DIR,
                f"erro_{script}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            )
            with open(erro_path, "w", encoding="utf-8") as f:
                f.write(resultado.stderr or "Erro sem mensagem no STDERR.")
            log(f"ERRO: {script} falhou. Execução interrompida.")
            raise Exception(f"Falha no script {script}")

        log(f"Finalizado: {script}")

    except Exception as e:
        log(f"Exceção capturada em {script}: {str(e)}")
        raise


# ============================================================
# VALIDAÇÃO DAS DIMENSÕES GOLD
# ============================================================

def validar_dimensoes():
    arquivos_dim = [
        os.path.join(ROOT, r"06_analitico_gold", "dim_classe_processamento.parquet"),
        os.path.join(ROOT, r"06_analitico_gold", "dim_empresa.parquet"),
        os.path.join(ROOT, r"06_analitico_gold", "dim_segmento_produto.parquet"),
    ]

    log("Validando dimensões GOLD...")

    faltando = [arq for arq in arquivos_dim if not os.path.exists(arq)]

    if faltando:
        erro_path = os.path.join(
            VALIDACAO_DIR,
            f"validacao_dimensoes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )
        with open(erro_path, "w", encoding="utf-8") as f:
            f.write("Arquivos de dimensão ausentes:\n")
            f.write("\n".join(faltando))

        log("ERRO: Dimensões GOLD não encontradas. Execução interrompida.")
        raise Exception("Dimensões GOLD ausentes.")

    log("Dimensões GOLD validadas com sucesso.")


# ============================================================
# LISTA DE SCRIPTS EM ORDEM (100% CORRETA)
# ============================================================

scripts = [
    "01_data_frame_python_ingestao.py",
    "02_process_03_data_frame_python_bronze.py",
    "04_data_frame_python_silver_1.py",
    "05_data_frame_python_silver_2.py",
    "06_01_data_frame_dim_gold.py",
    "06_02_data_frame_python_fato_gold.py",
]


# ============================================================
# EXECUÇÃO DO PIPELINE
# ============================================================

log("=== INÍCIO DA ORQUESTRAÇÃO ===")

try:
    for s in scripts:
        if s == "06_01_data_frame_dim_gold.py":
            executar(s)
            validar_dimensoes()
        else:
            executar(s)

    log("=== ORQUESTRAÇÃO FINALIZADA COM SUCESSO ===")

finally:
    if os.path.exists(LOCK_FILE):
        os.remove(LOCK_FILE)
        log("Lock removido. Orquestração liberada para próxima execução.")