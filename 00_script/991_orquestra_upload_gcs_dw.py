import os
from google.cloud import storage

# TESTE DE VIDA - TEM QUE APARECER NO CMD
print(">>> O SCRIPT 991 FOI INICIADO PELO PYTHON <<<")

# --- CONFIGURAÇÕES ---
CAMINHO_CREDENCIAIS = r"C:\Scripts\mini-lakehouse-uploader-dw.json" 
NOME_BUCKET = "mini-airflow-gold-data" 
PASTA_GOLD_LOCAL = r"G:\Meu Drive\mini-airflow\06_analitico_gold"

def iniciar_sincronizacao_dw():
    print("-" * 50)
    print("🛰️  INICIANDO ORQUESTRAÇÃO DE CLOUD (BATCH)")
    print("-" * 50)

    try:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CAMINHO_CREDENCIAIS
        client = storage.Client()
        bucket = client.bucket(NOME_BUCKET)

        arquivos = [f for f in os.listdir(PASTA_GOLD_LOCAL) if f.endswith('.parquet')]

        if not arquivos:
            print("⚠️ Nenhum arquivo .parquet encontrado na pasta Gold.")
            return

        for arquivo in arquivos:
            caminho_local = os.path.join(PASTA_GOLD_LOCAL, arquivo)
            blob = bucket.blob(arquivo)
            print(f"📦 Sincronizando: {arquivo}...")
            blob.upload_from_filename(caminho_local)
            print(f"✅ {arquivo} atualizado com sucesso!")

    except Exception as e:
        print(f"❌ ERRO CRÍTICO NO UPLOAD: {e}")

# ESSAS DUAS LINHAS ABAIXO NAO PODEM TER ESPAÇOS NO INÍCIO (ENCERADO NA ESQUERDA)
if __name__ == "__main__":
    iniciar_sincronizacao_dw()