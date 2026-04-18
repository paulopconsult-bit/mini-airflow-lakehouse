**🚀 Mini-Airflow Lakehouse: End-to-End Hybrid Pipeline**

**🏛️ Introdução**
Este projeto demonstra a implementação de um Lakehouse Híbrido funcional. A CPU local simula um servidor de processamento dedicado, automatizando o fluxo da Medallion Architecture sobre o Google Drive e sincronizando os resultados com o Google Cloud.

**🏗️ Arquitetura do Projeto**
O pipeline utiliza o hardware local como motor de cálculo para realizar a curadoria de dados em camadas:

**RAW** (Bronze): Ingestão de arquivos CSV sintéticos (gerados via script) com controle de logs, backup dos originais e manifesto de carga.

**SILVER** (Curadoria): Padronização técnica (Snake Case/ASCII) e aplicação de tipagem semântica profunda.

**GOLD** (Analítico): Modelagem Dimensional (Star Schema) com separação entre Fatos e Dimensões, incluindo métricas de negócio.

**CLOUD DW**: Upload automatizado para o Google Cloud Storage e consumo via BigQuery através de **Views** (Business) otimizadas.

**📂 Estrutura de Diretórios e Estratégia de Performance**
text

```
G:\MEU DRIVE\MINI-AIRFLOW (Google Drive Sincronizado Offline)
├── 00_script             <-- Maestro: Todos os scripts Python (ETL e Orquestração)
├── 01_entrada_raw        <-- Bronze Inbound: Landing zone dos arquivos CSV sintéticos
├── 02_processado_03_bronze  <-- Bronze Outbound: Dados consolidados em Parquet
├── 04_curadoria_silver   <-- Silver Transacional: Limpeza técnica e padronização
├── 05_business_silver    <-- Silver Business: Regras de negócio e tipagem
├── 06_analitico_gold     <-- Gold Camada: Tabelas Ouro (Fato e Dimensões)
└── 12_backup             <-- Recovery: Cópia de segurança dos arquivos originais

C:\SCRIPTS (Local Disk)
├── executar_orquestrador.bat  --> Disparador local para máxima velocidade no Windows
└── executar_upload_gcs.bat    --> Disparador local para acelerar a intercomunicação com o OS
```

**🛠️ Stack Tecnológica & Estratégia de Infra**

Compute: CPU Local operando como servidor de processamento automatizado.
Storage 

Híbrido: Uso do Google Drive sincronizado offline para processamento em velocidade de disco local.

Aceleração de Gatilhos: Scripts .bat em C:\SCRIPTS para reduzir latência no disparo das tarefas agendadas.

Orquestração: Python + Agendador de Tarefas do Windows (Execução automática a cada 4h).

Cloud DW: GCS (Storage) e BigQuery (Warehouse).

Visualização: Looker Studio Dashboard (Live).

**⚙️ Diferenciais Técnicos**
Sistema de Lock: Prevenção de concorrência via arquivos .lock.
Idempotência: Reprocessamento garantido sem corrupção ou duplicidade.
Analytics Engineering: Views no BigQuery com geolocalização e métricas calculadas em SQL.

**📝 Backlog de Evolução (Planned Features)**
Para otimização de recursos nesta fase inicial, as seguintes camadas foram projetadas e mapeadas na estrutura de pastas, mas aguardam implementação:
09_validacao: Implementação de testes de expectativa (Great Expectations) para Data Quality.
10_relatorios: Geração automática de PDFs com sumários de carga diária.
11_auditoria: Log detalhado de alteração de esquemas e acesso aos dados (Data Lineage avançado).

**🎯 Proposta e Escopo do Projeto**
O objetivo central deste repositório é a demonstração da lógica de engenharia e a ideação de negócio, focando na supervisão e integridade da infraestrutura.
Diferente de um sistema de produção real com dados orgânicos, este projeto opera como uma Prova de Conceito (PoC) onde:

* **Dados Sintéticos:** Os dados são gerados artificialmente e possuem um comportamento majoritariamente uniforme.

* **Foco na Arquitetura:** A prioridade é validar o "encanamento" dos dados (Data Pipeline), a resiliência da orquestração e a eficiência da infraestrutura híbrida.

* **Validação de Processo:** O foco está em como o dado atravessa as camadas de curadoria e aterrissa de forma íntegra no Data Warehouse, garantindo que a estrutura esteja pronta para suportar fluxos de dados reais e complexos.

---

## **📊 Entrega de Dados e Visualização (Dashboard Live)**

A ponta final do pipeline (Camada Gold) é consumida via **BigQuery Views** e visualizada no **Looker Studio**. 
O dashboard permite o monitoramento em tempo real da saúde da ingestão, volumetria de lotes e métricas de negócio por UF.

🔗 **[Acesse o Dashboard Interativo aqui](https://datastudio.google.com/reporting/1f5f3a7c-6ea2-4de7-9760-e2dcc91d8b6a)**

> **Monitoramento:** O painel reflete o status das cargas automatizadas pela CPU local e sincronizadas via **Cloud Storage**.

---