# ============================================================
# SCRIPT INGESTÃO data_frame_python
# ------------------------------------------------------------
# Gera arquivos RAW da ingestão "data_frame_python".
# Cria o primeiro arquivo ou pode escolher incrementar o último existente.
# Produz dados sintéticos padronizados para alimentar o pipeline.
#
# ============================================================
# 0. BIBLIOTECAS
# ------------------------------------------------------------
# Nesta seção importamos todas as bibliotecas necessárias para:
# - manipular arquivos e diretórios (os)
# - trabalhar com DataFrames (pandas)
# - capturar data e hora atual (datetime)
# - aguardar 1 segundo caso ocorra colisão de nomes (time)
# - gerar números aleatórios para os dados (random)
# ============================================================
import os
import time
import pandas as pd
from datetime import datetime
import random

# ============================================================
# 1. CONFIGURAÇÃO DO TIPO DE ENTRADA
# ------------------------------------------------------------
# Este script é responsável por gerar arquivos do tipo
# "data_frame_python". Declaramos o tipo aqui para que:
# - o nome dos arquivos siga um padrão consistente
# - o pipeline consiga identificar facilmente qual script
#   gerou qual arquivo
# - possamos criar outros scripts no futuro apenas mudando o TIPO
# ============================================================
TIPO = "data_frame_python"
EXTENSAO = ".csv"


# ============================================================
# 2. DEFINIÇÃO DA PASTA DE ENTRADA
# ------------------------------------------------------------
# Esta é a pasta onde os arquivos gerados serão colocados.
# Caso ela não exista, será criada automaticamente.
# Isso garante que o script nunca falhe por falta de diretório.
# ============================================================
PASTA_ENTRADA = r"G:\Meu Drive\mini-airflow\01_entrada_raw"
os.makedirs(PASTA_ENTRADA, exist_ok=True)


# ============================================================
# 3. FUNÇÃO: timestamp()
# ------------------------------------------------------------
# Gera uma string com data e hora atual no formato:
#   20260410_134522
# Esse formato é:
# - ordenável
# - limpo
# - sem caracteres especiais
# - ideal para pipelines
# ============================================================
def timestamp():
    return datetime.now().strftime("%Y%m%d_%H%M%S")


# ============================================================
# 4. FUNÇÃO: gerar_nome_unico()
# ------------------------------------------------------------
# Gera um nome de arquivo baseado no timestamp e no tipo.
# Caso o nome já exista (raro, mas possível se o script rodar
# duas vezes no mesmo segundo), o script aguarda 1 segundo e
# tenta novamente, garantindo 100% de unicidade.
# ============================================================
def gerar_nome_unico():
    while True:
        nome = f"{timestamp()}_{TIPO}{EXTENSAO}"
        caminho = os.path.join(PASTA_ENTRADA, nome)

        # Se o arquivo NÃO existe, retornamos imediatamente
        if not os.path.exists(caminho):
            return caminho

        # Se existir, esperamos 1 segundo e tentamos de novo
        time.sleep(1)


# ============================================================
# 5. FUNÇÃO: criar_primeiro()
# ------------------------------------------------------------
# Cria o PRIMEIRO arquivo do tipo data_frame_python.
#
# Estrutura do DataFrame:
# id_registro  -> AAAAMMDD_HHMMSS_XXXXXX
# id_pessoa    -> 1 a 100
# id_produto   -> 1 a 4
# classe       -> 1 a 3
# valor        -> 100 a 500
# gerado_em    -> timestamp da linha
#
# O timestamp base é extraído do nome do arquivo gerado.
# ============================================================
def criar_primeiro():
    # 1. Gera o nome único do arquivo
    caminho = gerar_nome_unico()

    # 2. Extrai o nome do arquivo sem caminho
    nome_arquivo = os.path.basename(caminho)

    # 3. Extrai o timestamp AAAAMMDD_HHMMSS do nome do arquivo
    timestamp_base = nome_arquivo.split("_data_frame_python")[0]

    # 4. Cria 3 registros iniciais
    registros = []
    for i in range(1, 100001):
        id_registro = f"{timestamp_base}_{i:06d}"
        registros.append({
            "id_registro": id_registro,
            "id_pessoa": random.randint(1, 100),
            "id_produto": random.randint(1, 4),
            "classe": random.randint(1, 3),
            "valor": round(random.uniform(100, 500), 2),
            "gerado_em": datetime.now()
        })

    # 5. Converte para DataFrame
    df = pd.DataFrame(registros)

    # 6. Salva o arquivo
    df.to_csv(caminho, index=False, encoding="utf-8")

    print("Primeiro arquivo criado:", caminho)


# ============================================================
# 6. FUNÇÃO: incrementar()
# ------------------------------------------------------------
# Adiciona uma nova linha ao DataFrame existente.
#
# Mantém o mesmo timestamp base do arquivo para gerar
# id_registro no padrão AAAAMMDD_HHMMSS_XXXXXX.
# ============================================================
def incrementar(caminho):
    # 1. Lê o arquivo existente
    df = pd.read_csv(caminho)

    # 2. Extrai o timestamp base do nome do arquivo
    nome_arquivo = os.path.basename(caminho)
    timestamp_base = nome_arquivo.split("_data_frame_python")[0]

    # 3. Pega o último id_registro
    ultimo_id = df["id_registro"].iloc[-1]

    # 4. Extrai o contador (últimos 6 dígitos)
    ultimo_contador = int(ultimo_id.split("_")[-1])

    # 5. Incrementa o contador
    novo_contador = ultimo_contador + 1

    # 6. Monta o novo id_registro
    novo_id_registro = f"{timestamp_base}_{novo_contador:06d}"

    # 7. Cria a nova linha
    nova_linha = {
        "id_registro": novo_id_registro,
        "id_pessoa": random.randint(1, 100),
        "id_produto": random.randint(1, 4),
        "classe": random.randint(1, 3),
        "valor": round(random.uniform(100, 500), 2),
        "gerado_em": datetime.now()
    }

    # 8. Adiciona ao DataFrame
    df = pd.concat([df, pd.DataFrame([nova_linha])], ignore_index=True)

    # 9. Salva o arquivo
    df.to_csv(caminho, index=False, encoding="utf-8")

    print("Arquivo incrementado:", caminho)


# ============================================================
# 7.1 MODO INCREMENTAL DE GERAÇÃO DE ARQUIVOS
# ------------------------------------------------------------
# 1. Lista todos os arquivos da pasta de entrada.
# 2. Filtra apenas os arquivos que pertencem a este tipo.
# 3. Se não existir nenhum → cria o primeiro arquivo.
# 4. Se existir → pega o mais recente e incrementa.
#
# Observação:
# Como os nomes começam com timestamp, ordenar alfabeticamente
# já coloca o mais recente por último.
# ============================================================
# arquivos = [f for f in os.listdir(PASTA_ENTRADA) if TIPO in f]

# if not arquivos:
#     criar_primeiro()

# else:
#     arquivo = sorted(arquivos)[-1]
#     caminho = os.path.join(PASTA_ENTRADA, arquivo)

#     df = pd.read_csv(caminho)

#     if df.empty:
#         criar_primeiro()
#     else:
#         incrementar(caminho)


# ============================================================
# 7.2 MODO APENAS CRIAR NOVO ARQUIVO
# ============================================================
criar_primeiro()
