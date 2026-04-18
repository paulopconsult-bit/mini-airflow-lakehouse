# ============================================================
# SCRIPT 06_01 — CRIAÇÃO DAS DIMENSÕES DO GOLD
# ============================================================

import pandas as pd
from pathlib import Path

BASE = Path(r"G:\Meu Drive\mini-airflow")
PASTA_GOLD = BASE / "06_analitico_gold"
PASTA_GOLD.mkdir(exist_ok=True)

# ============================================================
# DIMENSÃO EMPRESA — 100 EMPRESAS REAIS COM UF REAL
# ============================================================

empresas = [
    # SUDESTE (40)
    ("AlfaTech", "SP"), ("NovaLog", "SP"), ("Orion Telecom", "SP"), ("PrimeBank", "SP"),
    ("UrbanPay", "SP"), ("BlueWave", "SP"), ("DataSphere", "SP"), ("CloudBridge", "SP"),
    ("NextVision", "SP"), ("SkyLink", "SP"),
    ("MetaCargo", "RJ"), ("FusionWare", "RJ"), ("BrightLabs", "RJ"), ("CoreSystems", "RJ"),
    ("HyperSoft", "RJ"), ("StreamHub", "RJ"), ("WaveNet", "RJ"), ("NeonWorks", "RJ"),
    ("QuantumPay", "MG"), ("IronData", "MG"), ("MegaLink", "MG"), ("SolarFoods", "MG"),
    ("GreenFarm", "MG"), ("AgroPlus", "MG"), ("EcoMining", "MG"), ("TerraNova", "MG"),
    ("VortexTech", "ES"), ("PrimeOcean", "ES"), ("EcoWave", "ES"), ("BrightSea", "ES"),
    ("OceanLink", "ES"), ("BlueHarbor", "ES"), ("PortoDigital", "ES"), ("HarborTech", "ES"),
    ("SeaBridge", "ES"), ("AtlanticData", "ES"),

    # 4 EMPRESAS QUE FALTAVAM NO SUDESTE
    ("TechMasters", "SP"),
    ("InfoLink Solutions", "RJ"),
    ("AgroMineral", "MG"),
    ("PortoWave", "ES"),

    # SUL (20)
    ("SouthTech", "PR"), ("AgroLink", "PR"), ("ParanaData", "PR"), ("GreenLog", "PR"),
    ("UrbanFoods", "PR"), ("NextAgro", "PR"), ("PrimeSul", "SC"), ("OceanSoft", "SC"),
    ("BlueFarm", "SC"), ("SantaCloud", "SC"), ("SulMining", "RS"), ("GaiaTech", "RS"),
    ("PortoSul", "RS"), ("AgroSul", "RS"), ("DataSul", "RS"), ("StreamSul", "RS"),
    ("SulConnect", "RS"), ("FarmLink", "RS"), ("EcoSul", "RS"), ("BrightSul", "RS"),

    # NORDESTE (20)
    ("NordTech", "BA"), ("BahiaFoods", "BA"), ("AgroBahia", "BA"), ("OceanBahia", "BA"),
    ("NordLog", "PE"), ("PernambucoData", "PE"), ("StreamNord", "PE"), ("BrightNord", "PE"),
    ("CearaTech", "CE"), ("AgroCeara", "CE"), ("NordWave", "CE"), ("RioNord", "RN"),
    ("NatalTech", "RN"), ("ParaibaData", "PB"), ("NordConnect", "PB"), ("AlagoasTech", "AL"),
    ("SergipeCloud", "SE"), ("MaranhaoData", "MA"), ("PiauiDigital", "PI"), ("NordMining", "PI"),

    # CENTRO-OESTE (10)
    ("CentroTech", "DF"), ("CapitalData", "DF"), ("PlanaltoCloud", "DF"), ("GoiasTech", "GO"),
    ("AgroGoias", "GO"), ("CerradoFoods", "GO"), ("MatoTech", "MT"), ("AgroMT", "MT"),
    ("PantanalData", "MS"), ("MSConnect", "MS"),

    # NORTE (10)
    ("AmazonTech", "AM"), ("ManausData", "AM"), ("AgroAmazon", "AM"), ("ParaDigital", "PA"),
    ("BelemTech", "PA"), ("RoraimaCloud", "RR"), ("AmapaData", "AP"), ("AcreTech", "AC"),
    ("TocantinsDigital", "TO"), ("RondoniaSystems", "RO")
]

# Garantia absoluta: 100 empresas
assert len(empresas) == 100, f"Lista contém {len(empresas)} empresas, mas deveria conter 100."

df_empresa = pd.DataFrame(empresas, columns=["nome_empresa", "uf"])
df_empresa["id_empresa"] = range(1, 101)
df_empresa = df_empresa[["id_empresa", "nome_empresa", "uf"]]

# ============================================================
# DIMENSÃO SEGMENTO PRODUTO
# ============================================================
df_segmento = pd.DataFrame({
    "id_segmento": [1, 2, 3, 4],
    "nome_segmento": ["Lata", "Bronze", "Prata", "Ouro"]
})

# ============================================================
# DIMENSÃO CLASSE PROCESSAMENTO
# ============================================================
df_classe = pd.DataFrame({
    "id_classe": [1, 2, 3],
    "nome_classe": ["Online", "Streaming", "Batch"],
    "fator_classe": [0.35, 0.15, 0.05]
})

# ============================================================
# SALVAR AS DIMENSÕES
# ============================================================
df_empresa.to_parquet(PASTA_GOLD / "dim_empresa.parquet", index=False)
df_segmento.to_parquet(PASTA_GOLD / "dim_segmento_produto.parquet", index=False)
df_classe.to_parquet(PASTA_GOLD / "dim_classe_processamento.parquet", index=False)