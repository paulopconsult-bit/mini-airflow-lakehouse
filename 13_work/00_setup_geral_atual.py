"""
00_setup_geral.py
------------------

Script de SETUP do projeto mini-airflow.

Objetivo:
    - Criar a Estrutura de Diretórios numerada
    - Validar se as pastas existem
    - Preparar o ambiente para os demais scripts
    - NÃO faz parte da orquestração futura

Este script deve ser executado MANUALMENTE sempre que:
    - O projeto for criado do zero
    - A estrutura precisar ser recriada
    - Novas pastas forem adicionadas

Autor: Paulo Dias
"""

import os

# ============================================================
# 1) Caminho base do projeto (ajuste se necessário)
# ============================================================

BASE_PATH = r"G:\Meu Drive\mini-airflow"

# ============================================================
# 2) Estrutura de Diretórios oficial (numerada)
# ============================================================

PASTAS = [
    "00_scripts"
    "01_entrada",
    "02_processando",
    "03_processado",
    "04_erros",
    "05_logs",
    "06_relatorios",
    "07_backup",
    "08_validacao"
]

# ============================================================
# 3) Função para criar pastas
# ============================================================

def criar_pastas():
    print("\n🔧 Iniciando criação da Estrutura de Diretórios...\n")

    # Verifica se a pasta raiz existe
    if not os.path.exists(BASE_PATH):
        print(f"❌ ERRO: A pasta raiz não existe:\n{BASE_PATH}")
        print("Crie manualmente antes de rodar o setup.")
        return

    # Cria cada pasta da lista
    for pasta in PASTAS:
        caminho = os.path.join(BASE_PATH, pasta)

        if not os.path.exists(caminho):
            os.makedirs(caminho)
            print(f"📁 Criada: {caminho}")
        else:
            print(f"✔ Já existe: {caminho}")

    print("\n🎉 Estrutura criada/validada com sucesso!\n")

# ============================================================
# 4) Execução principal
# ============================================================

if __name__ == "__main__":
    criar_pastas()