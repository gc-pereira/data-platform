import boto3
import numpy as np
import pandas as pd
import awswrangler as wr

from datetime import datetime
from dateutil.relativedelta import relativedelta


def gerar_dados_contratos(qtd=1000):
    np.random.seed(42)  # opcional, deixa os valores reprodutíveis

    data = {
        "contract_id": np.random.randint(0, 100000, qtd),
        "customer_id": np.random.randint(0, 1000, qtd),
        "valor_total": np.round(np.random.uniform(100.0, 50000.0, qtd), 2)
    }

    df = pd.DataFrame(data)
    return df


def salvar_no_s3(df, days):
    caminho = "s3://sor-data-custodia/tb_dados_externos/"
    
    df['last_update'] = datetime.today() - relativedelta(days=days)
    df['anomesdia'] = df['last_update'].dt.strftime('%Y%m%d').astype(int)

    wr.s3.to_parquet(
        df=df,
        path=caminho,
        dataset=True,       
        table=caminho.split('/')[-2] ,
        database='sor',
        partition_cols=['anomesdia'],
        mode="overwrite_partitions"
    )

    print(f"Dataset salvo em: {caminho}")


# -------------------------
# Execução
# -------------------------

if __name__ == "__main__":
    for i in range(1):
        df = gerar_dados_contratos()
        salvar_no_s3(df, days=i)
