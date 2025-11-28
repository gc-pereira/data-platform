import boto3
import random
import numpy as np
import pandas as pd
import awswrangler as wr

from faker import Faker
from datetime import datetime, timedelta

fake = Faker("pt_BR")

# Quantidade de registros
N = 500

status_list = ["Ativo", "Inadimplente", "Quitado", "Cancelado"]
produtos = ["Crédito Pessoal", "Financiamento", "Cartão", "Consignado", "Imobiliário"]

data = []

for i in range(N):
    created_at = fake.date_time_between(start_date="-2y", end_date="now")
    updated_at = created_at + timedelta(days=random.randint(0, 700))

    valor_total = round(random.uniform(500, 50000), 2)
    valor_juros = round(valor_total * random.uniform(0.05, 0.3), 2)
    valor_iof = round(valor_total * random.uniform(0.005, 0.03), 2)

    parcelas_totais = random.randint(6, 72)
    parcelas_quitadas = random.randint(0, parcelas_totais)
    
    contrato = np.random.randint(0, 100000)

    data.append({
        "contract_id": contrato,
        "Produto": random.choice(produtos),
        "Status": random.choice(status_list),
        "Timestamp_Criacao": created_at,
        "DataAtualizacao": updated_at,
        "Valor_Total": valor_total,
        "Valor_Juros": valor_juros,
        "Valor_IOF": valor_iof,
        "Parcelas_Totais": parcelas_totais,
        "Parcelas_Quitadas": parcelas_quitadas,
        "customer_id": fake.uuid4(),
        "Nome": fake.name(),
        "CriacaoRegistro": datetime.today().strftime('%Y%m%d')
    })

df = pd.DataFrame(data)

wr.s3.to_csv(
    df=df,
    path=f"s3://landing-data-custodia/tb_contratos_mainframe/CONTRATOS_{datetime.today().strftime('%Y%m%d')}.csv",
    index=False,
    header=False,
)

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table('tb_cntt_app_mble')

for item in data:
    for key in item.keys():
        item[key] = str(item[key])
    table.put_item(
        Item=item
    )
    
    print('registro salvo')
    print(item)
    print('='*10)