**🧠 Algoritmo de Processamento: Mini-Airflow Lakehouse**

**🏁 Nível 0:** Os Gatilhos (Unidade C:Script)
O processo não nasce no Python, mas no sistema operacional via Windows Task Scheduler.
Trigger A (T+5min login): Dispara o executar_orquestrador.bat.
Trigger B (T+30min login): Dispara o executar_upload_gcs.bat.
Nota: Ambos rodam a cada 4 horas para manter o ciclo de atualização.

**⚙️ Nível 1:** Orquestração Local (Script 99)
O arquivo 99_orquestra.py é o cérebro que gerencia a fila.
Check de Lock: Verifica se existe o arquivo .lock. Se sim, aborta (evita colisão).
Criação de Lock: Se não existe, cria o .lock para proteger a execução atual.
Loop Sequencial: Chama os scripts de 01 a 06_02 via subprocess.

**🏗️ Nível 2:** O Pipeline de Dados (Fluxo Medallion)

**1.** Ingestão (Script 01):
Gera dados sintéticos (CSV).
Garante nomes únicos baseados em timestamp.
Decide entre criar novo arquivo ou incrementar o último.

**2.** Camada Bronze (Script 02):
Estabilização: Checa se o arquivo não está sendo usado pelo Drive.
Staging: Move do RAW para a pasta 02_processando.
Consolidação: Lê o CSV, converte para Parquet e une ao histórico.
Auditabilidade: Gera o backup original e atualiza o manifesto_ingestao.csv.

**3.** Camada Silver Curadoria (Script 04):
Renomeação: Padroniza colunas para snake_case.
Limpeza Técnica: Remove acentos, espaços e caracteres especiais (Normalização NFKD).

**4.** Camada Silver Business (Script 05):
Tipagem: Garante IDs como string, datas como datetime e valores como numeric.
Regras de Negócio: Filtra valores negativos, remove duplicatas por id_registro e extrai colunas de tempo (ano/mês).

**5.** Camada Gold (Scripts 06_01 e 06_02):
Dimensões (06_01): Cria tabelas de referência (Empresa, Produto, Classe).
Fato (06_02): Realiza o JOIN entre os dados da Silver Business e as Dimensões. Calcula o Valor Ponderado (métrica final).

**☁️ Nível 3:** Sincronização Cloud (Script 991)
Este script atua de forma independente ou ao final da orquestração.
Autenticação: Carrega a Service Account JSON no ambiente.
Sincronização: Compara e faz o upload dos arquivos Parquet da pasta 06_analitico_gold para o Google Cloud Storage (Bucket).

**📊 Nível 4:** Consumo e Visualização
BigQuery: O Cloud Storage atualiza as tabelas físicas.
View SQL: A vw_fato_gold_view consolida os JOINs e prepara a geolocalização.
Looker Studio: Consome a View para gerar o dashboard de monitoramento e negócios.

**🛠️ Resumo de Manutenção**
Logs de Erro: Se falhar, olhe a pasta 07_erros.
Logs de Sucesso: Histórico completo na pasta 08_logs.

**Reset Manual:** Se o script travar, apague o arquivo .lock na pasta 00_script.
