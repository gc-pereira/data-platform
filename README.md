## Parte 1: Resultados, Objetivos e Governança

A parte 1 não será desenvolvida neste documento devido compliance.

### Aplicação do Data Mesh na Gestão de Contratos Financeiros Pós-Migração

O **Data Mesh** é a arquitetura ideal para a gestão de contratos financeiros, pois transforma os dados em **Produtos de Dados** gerenciados por times de domínio. Na custódia de contratos, o domínio natural é o **Domínio de Custódia de Contratos e Clientes**.

#### Estratégia e Conceitos Chave

1.  **Organização por Domínio (Descentralização):**

      * **Domínio de Custódia de Contratos:** É responsável de ponta a ponta (ingestão, transformação, exposição) pelos dados de contrato (`valor_total`, `status`, `parcelas`).
      * **Times de Domínio:** O time de engenharia de dados do domínio se torna o *Product Owner* dos dados, garantindo sua qualidade, frescor e governança. Isso reduz a dependência do time central (o antigo "Datalake central").

2.  **Dados como Produto (Data as a Product):**

      * **Data Product:** O dado de contrato não é apenas um *dataset*, mas um **produto**. O **Data Product: Contratos Financeiros** é exposto no AWS S3 (SoT/Spec) em formato Parquet/Iceberg.
      * **Características Essenciais (ADAM):**
          * **Addressable (Endereçável):** Acessível via um *endpoint* padrão (Glue Catalog/Athena).
          * **Discoverable (Descobrível):** Catalogado no AWS Glue Data Catalog, com metadados ricos (dicionário de dados, SLAs).
          * **Accessible (Acessível):** Acesso controlado via IAM e *Resource Link* do Glue (Governança Federada).
          * **Trustworthy (Confiável):** Possui *SLOs de Qualidade* (Data Quality) definidos e monitorados via **Glue Data Quality**.

3.  **Plataforma de Dados Self-Service (AWS como Enabler):**

      * O time de engenharia centraliza as ferramentas (AWS Glue, S3, Lambda, IAM) e automatiza a criação de *pipelines* padronizados (CI/CD, terraform/CloudFormation).
      * **Exemplo:** O time de Contratos solicita um novo *bucket* S3, um *job* Glue e regras de *Data Quality*, e a plataforma de dados provisiona tudo automaticamente.

4.  **Governança Federada:**

      * **Padrões Globais:** A governança central define padrões globais (ex: formato Parquet para exposição, uso obrigatório de *Data Quality* com Glue Data Quality).
      * **Execução Local:** O time do Domínio de Contratos tem a autonomia para escolher como modelar internamente os dados, desde que atenda aos padrões globais de exposição e qualidade. A segurança é gerenciada via **AWS Lake Formation/IAM**.

-------------------------------------------------------------------
## Parte 2: Testes Práticos de Codificação e Pensamento Crítico

### Job de Migração de Contratos (PySpark/Glue)

A solução será implementada usando **PySpark** no **AWS Glue**, visando a robustez e otimização. Será apresentada essa seção utilizando a plataforma de dados criada nesse mesmo repositório. Para iniciar, é necessário a construção de um arquivo de configuração que ditará de onde o dado será lido e como ele será escrito.

```json
{
    "DataLayer": "CSV",
    "TableName": "tb_contratos_mainframe",
    "DatabaseName": "sor",
    "Machine": {
        "Capacity": 2,
        "Type": "G.1X"
    },
    "Dependencies": [
        {
            "TableName": "CONTRATOS_%Y%m%d.csv",
            "DatabaseName": "",
            "Predicate": [
            ],
            "Quality": {
                "Rules":[
                    "RowCount > 1000"
                ],
                "ReadIfFail": true
            }
        }
    ],
    "Quality": {
        "Rules": [
            "RowCount > 1000",
            "ColumnValues \"status\" in [\"Ativo\", \"Inadimplente\", \"Quitado\", \"Cancelado\"]"
        ],
        "WriteIfFail": true
    }
}
```

o job que será executado é escrito como,
```python
from datacustodia.pipeline import JobPipeline

if __name__ == "__main__":
    job_pipeline = JobPipeline(
        args=["table_name"]
    )
    job_pipeline.extract()
    job_pipeline.transform()
    job_pipeline.posdq()
    job_pipeline.write()
    job_pipeline.update_partitions()
    job_pipeline.idempotence()
```
e seu código fonte pode ser encontrado diretamente neste repositório utilizando em
```python
datacustodia/src/datacustodia/pipeline.py
```

<p align="center">
    <img src="images/dq.gif" alt="Data Quality validation (dq.gif)" />
</p>

a principal ideia é criar um fluxo que utilize poucas configurações para funcionamento e a plataforma cuide de problemas como small files e atualização das partições.

### Otimização de Carga Incremental com Glue e S3

A chave para a carga incremental otimizada é a combinação de **filtro de metadados** (Predicado Pushdown) e o uso de **Delta Lake** ou **Merge/Upsert** no Glue.

#### Identificar Dados Novos/Modificados

  * **Lógica:** O job deve buscar a **máxima `last_update_date`** processada com sucesso na execução anterior.
  * **Implementação:**
      * Armazene a `max_last_update_date` em um **AWS DynamoDB** (chave `tabela_contratos`) ou no **AWS Parameter Store**.
      * Na nova execução, filtre a fonte (`source_df`) onde `last_update_date` é **maior que** a data armazenada.

#### Particionamento no S3

  * **Estrutura:** `s3://sot-data-custodia/tb_unindo_diferentes_fontes/ano_mes=YYYYMM/tipo_contrato=XXX/`
  * **Otimização:** A coluna **`tipo_contrato`** deve ser usada, pois é uma *coluna de baixa cardinalidade* e é frequentemente usada como filtro em consultas de negócio. A coluna **`ano_mes`** permite o *pruning* histórico.

#### Otimizar o Glue Job (Evitar Full Scan)

  * **Predicate Pushdown (Particionamento do S3):** Para a leitura do *dataset* anterior (Curated Zone), use o `catalog_table` com `push_down_predicate` no Glue.

      * **Exemplo:** Se a tabela estiver particionada por `ano_mes`, e a lógica incremental precisa verificar chaves existentes, você pode otimizar a leitura do destino, lendo apenas as partições recentes que seriam afetadas.

#### Garantir Idempotência (Evitar Duplicação)

Para garantir que o processo execute somente uma vez, a data de processamento e partições executadas serão salvar em banco dynamoDB, e seu código tambem pode ser encontrado em 
```python
datacustodia/src/datacustodia/services/idempotencia.py
```
<p align="center">
    <img src="images/idempotencia.gif" alt="Idempotência do job (idempotencia.gif)" />
</p>


#### Processas multiplas fontes de dado
Também utilizando a plataforma de dados, conseguimos criar os seguintes artefatos, que consistem em um JSON de configuração onde teremos todas as informações de Pré e Pós Data Quality, além de informações sobre o nome da tabela e database, push_down_predicate. No JSON a seguir é feita a leitura de três tabelas, origem mainframe, origem dynamo e origem parquet no s3.
```json
{
    "DataLayer": "SOT",
    "TableName": "tb_unindo_diferentes_fontes",
    "DatabaseName": "sot",
    "Machine": {
        "Capacity": 2,
        "Type": "G.1X"
    },
    "Dependencies": [
        {
            "TableName": "tb_contratos_mainframe",
            "DatabaseName": "sor",
            "Predicate": [
                {
                    "PushDown": "last_update_date >= '{} 00:00:00.000'",
                    "Type": "Date",
                    "Filter": -2,
                    "Pattern": "%Y-%m-%d"
                },
                {
                    "PushDown": "last_update_date < '{} 23:59:59.000'",
                    "Type": "Date",
                    "Filter": 0,
                    "Pattern": "%Y-%m-%d"
                }
            ],
            "Quality": {
                "Rules":[
                    "DistinctValuesCount \"contract_id\" > 1",
                    "RowCount > 100"
                ],
                "ReadIfFail": true
            }
        },
        {
            "TableName": "tb_contratos_mobile",
            "DatabaseName": "sor",
            "Predicate": [
                {
                    "PushDown": "anomesdia > {}",
                    "Type": "Date",
                    "Filter": -10,
                    "Pattern": "%Y%m%d"
                },
                {
                    "PushDown": "anomesdia < {}",
                    "Type": "Date",
                    "Filter": 0,
                    "Pattern": "%Y%m%d"
                }
            ],
            "Quality": {
                "Rules":[
                    "DistinctValuesCount \"contract_id\" > 1",
                    "RowCount > 100"
                ],
                "ReadIfFail": true
            }
        },
        {
            "TableName": "tb_dados_externos",
            "DatabaseName": "sor",
            "Predicate": [
                {
                    "PushDown": "anomesdia > {}",
                    "Type": "Date",
                    "Filter": -10,
                    "Pattern": "%Y%m%d"
                },
                {
                    "PushDown": "anomesdia < {}",
                    "Type": "Date",
                    "Filter": 0,
                    "Pattern": "%Y%m%d"
                }
            ],
            "Quality": {
                "Rules":[
                    "DistinctValuesCount \"contract_id\" > 1",
                    "RowCount > 100"
                ],
                "ReadIfFail": true
            }
        }
    ],
    "Quality": {
        "Rules": [
            "ColumnValues \"fonte\" in [\"MAINFRAME\", \"MOBILE\", \"EXTERNOS\"]",
            "RowCount > 1000"
        ],
        "WriteIfFail": true
    }
}
```
Também é necessário o sparkSQL que dará forma a toda transformação necessária nos dados
```sql
SELECT 
    CAST(CONTRACT_ID AS STRING) AS CONTRACT_ID,
    CAST(ID_CLIENTE AS STRING) AS CUSTOMER_ID,
    SUM(VALOR_TOTAL) AS VALOR,
    'MAINFRAME' AS FONTE,
    DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd') AS DATA_PROCESSAMENTO
FROM TB_CONTRATOS_MAINFRAME
GROUP BY 
    CONTRACT_ID, ID_CLIENTE

UNION

SELECT 
    CAST(CONTRACT_ID AS STRING) AS CONTRACT_ID,
    CAST(CUSTOMER_ID AS STRING) AS CUSTOMER_ID,
    SUM(CAST(VALOR_TOTAL AS DECIMAL(38,10))) AS VALOR,
    'MOBILE' AS FONTE,
    DATE_FORMAT(CURRENT_DATE(), 'yyyyMMdd') AS DATA_PROCESSAMENTO
FROM TB_CONTRATOS_MOBILE
GROUP BY 
    CONTRACT_ID, CUSTOMER_ID

UNION

SELECT 
    CAST(CONTRACT_ID AS STRING) AS CONTRACT_ID,
    CAST(CUSTOMER_ID AS STRING) AS CUSTOMER_ID,
    SUM(VALOR_TOTAL) AS VALOR,
    'EXTERNOS' AS FONTE,
    DATE_FORMAT(LAST_UPDATE, 'yyyyMMdd') AS DATA_PROCESSAMENTO
FROM TB_DADOS_EXTERNOS
GROUP BY 
    CONTRACT_ID, CUSTOMER_ID, DATE_FORMAT(LAST_UPDATE, 'yyyyMMdd')

```
O código tambem executará dentro da plataforma
```python
from datacustodia.pipeline import JobPipeline

if __name__ == "__main__":
    job_pipeline = JobPipeline(
        args=["table_name"]
    )
    job_pipeline.extract()
    job_pipeline.transform()
    job_pipeline.posdq()
    job_pipeline.write()
    job_pipeline.update_partitions()
    job_pipeline.idempotence()
```
e seu resultado pode ser encontrado na imagem a seguir

<p align="center">
    <img src="images/dynamo.gif" alt="Idempotência do job (idempotencia.gif)" />
</p>

### Otimização de Custo com Compactação e Partition Pruning

O problema de **custo alto** e **scans caros no Athena** é classicamente resolvido com **formato colunar**, **compactação**, e o manejo do **"Small Files Problem"**.

O script original deve ser ajustado para incorporar o reescrever dos dados (compactação/reparticionamento) e a melhoria na estrutura de particionamento.

```python
import boto3

from pyspark.sql import functions as F
from pyspark.context import SparkContext

from awsglue.context import GlueContext

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

### CONFIGURAÇÕES ####
S3_BUCKET = "sor-data-custodia"
TABLE_PATH = "s3://sor-data-custodia/tb_contatos_mainframe/"
THRESHOLD_SMALL_MB = 32     # small files = menores que isso
TARGET_MIN_MB = 256         # arquivo final mínimo
TARGET_MAX_MB = 1024        # arquivo final máximo
PARTITION_COL = "dt"

s3 = boto3.client("s3")


def list_s3_files(prefix):
    bucket = S3_BUCKET
    paginator = s3.get_paginator("list_objects_v2")
    paths = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size = obj["Size"]
            paths.append((key, size))

    return paths


def compact_partition(partition_value):
    print(f"\n=== Compactando partição: {partition_value} ===")

    prefix = f"minha-tabela/{PARTITION_COL}={partition_value}/"
    files = list_s3_files(prefix)

    small_files = [(k, size) for k, size in files if size < THRESHOLD_SMALL_MB * 1024 * 1024]

    if not small_files:
        print("Nenhum small file encontrado.")
        return

    print(f"{len(small_files)} small files encontrados.")

    # Arquivos em full path
    paths = [f"s3://{S3_BUCKET}/{k}" for k, _ in small_files]

    total_bytes = sum(size for _, size in small_files)
    total_mb = total_bytes / (1024 * 1024)

    print(f"Tamanho total dos small files: {round(total_mb, 2)} MB")

    # Cálculo da quantidade ideal de arquivos finais
    # Queremos arquivos entre 256MB e 1GB
    ideal_num_files = max(1, int(total_mb / TARGET_MAX_MB))
    max_num_files = max(1, int(total_mb / TARGET_MIN_MB))

    # Garantir que não gere arquivos muito pequenos
    num_final_files = max(ideal_num_files, 1)
    num_final_files = min(num_final_files, max_num_files)

    print(f"Gerando {num_final_files} arquivos finais...")

    df = spark.read.parquet(*paths)

    # Cria os arquivos no tamanho desejado
    (
        df.repartition(num_final_files)
          .write
          .mode("overwrite")
          .parquet(f"{TABLE_PATH}{PARTITION_COL}={partition_value}/")
    )

    print("Compactado com sucesso.")


if __name__ == "__main__":
    df = spark.read.parquet(TABLE_PATH)
    particoes = [row[PARTITION_COL] for row in df.select(PARTITION_COL).distinct().collect()]

    print("Partições encontradas:", particoes)


    for p in particoes:
        compact_partition(p)

    spark.stop()
```

## Parte 3: Otimização e Fine Tunning

### Particionamento de Dados de Contratos no S3 para Otimizar Consultas

O particionamento deve focar em **colunas de alta cardinalidade no filtro** e **baixa cardinalidade na coluna em si**, sempre mantendo o tamanho de arquivo entre **256MB** e **1GB**.

#### 1\. Critérios de Particionamento:

  * **Chave Principal (Filtro de controle): `anomesdia_processamento`:**

      * **Justificativa:** Considerando um cenário onde dados chegam de forma full em uma tabela, devemos considerar a data de processamento como partição com intuito de saber qual a ultima foto dos contratos de crédito porém essa não deve ser a única partição pois não trás valor analítico.

  * **Chave Secundária (Filtro mais seletivo): `anomesdia_contratação`:**

      * **Justificativa:** A maioria das consultas analíticas é temporal e restrita a janelas recentes (ex: "Contratos criados no último trimestre"). Particionar por **`ano_mes`** permite o *pruning* do histórico (terabytes de dados antigos).

#### 2\. Estrutura no S3:

A estrutura de pasta recomendada, seguindo o padrão Hive, é:

```python
s3://sot-data-custodia/contratos/anomesdia_processamento=YYYY-MM-DD/anomesdia_contratacao=YYYY-MM-DD/
```


#### 3\. Benefícios:

| Benefício | Descrição |
| :--- | :--- |
| **Partition Pruning (Corte de Partição)** | O Athena/Spark ignora pastas/arquivos que não satisfazem o filtro `WHERE`, **reduzindo drasticamente o volume de dados escaneados** e, consequentemente, o custo. |
| **Melhoria de Performance** | Menos dados escaneados e *metadata* lida (via Glue Catalog) resulta em **consultas muito mais rápidas**. |
| **Otimização de Custos** | No Athena, o custo é baseado no volume de dados escaneados. O *Pruning* é o principal fator de **redução de custo**. |
## Parte 4: Estrutura de Dados

### Visão Geral

- **Problema**: Necessidade de armazenar histórico de alterações de contratos.
- **Objetivo**: Frequentemente tem-se a necessidade de verificar dados de versões antigas de contratos para rastreabilidade e estudos.
- **Como resolver:** Podemos guardar o histórico de contratos de duas formas. Uma forma é guardar o dado particionado pela data de processamento e data de contratação e a segunda é a utilização de tabelas Iceberg que permitem o time travel.

------------------------------------------------------------------------
### Por que Iceberg é ideal para histórico de contratos?

-   **Time Travel** para consultar qualquer versão histórica.
-   **Partition Evolution** para ajustar partições sem recriar tabela.
-   **MERGE INTO avançado** para SCD1 e SCD2.
-   **Metadados escaláveis** que evitam `file listing`.
-   **Integração nativa com AWS Athena + Glue Catalog**.
-   **Compatível com Spark, Flink, Trino, Presto, Snowflake, EMR,
    Athena**.
- **Guarda a última melhor foto** otimizando a consulta de dados.

------------------------------------------------------------------------

### Arquitetura recomendada para histórico de contratos

             [Sistema Origen]
                   |
            Arquivo FULL diário
                   |
            ┌───────────────────────┐
            │      Glue Job 1       │
            │  - Padronização       │
            │  - Limpeza DQ         │
            │  - Enriquecimento     │
            └─────────┬─────────────┘
                      |
                      v
            [Camada SoR]
            Tabela: contracts_sor
            Partições: data_processamento e data_contratacao 
                      |
            ┌───────────────────────┐
            │      Glue Job 2       │
            │  - Deduplicação       │
            │  - Merge Iceberg      │
            │  - SCD1 / SCD2        │
            └─────────┬─────────────┘
                      |
                      v
           [Camada SoT - Iceberg]
           Tabela: contracts_sot
           Partições: days(created_at), bucket(N, contract_id)
                      |
            ┌───────────────────────┐
            │      Glue Job 3       │
            │  - Lê Snapshots       │
            │  - Identifica contratos 
            |  que foram atualizados│
            └─────────┬─────────────┘
                      |
                      v
           [Camada SoT - Iceberg]
           Tabelas: tb_aux_primeira_insercao, tb_aux_ultima_atualizacao
           Partições: contract_id
------------------------------------------------------------------------

### Criação de tabelas Iceberg para histórico

#### Tabela SoR (camada bruta)

``` sql
CREATE EXTERNAL TABLE `tb_contratos_mainframe`(
  `contract_id` string, 
  `produto` string, 
  `status` string, 
  `updated_at` timestamp, 
  `valor_total` double, 
  `valor_juros` double, 
  `valor_iof` double, 
  `parcelas_totais` int, 
  `parcelas_quitadas` int, 
  `id_cliente` string, 
  `nome` string)
PARTITIONED BY ( 
  `last_update_date` timestamp,
  `created_at` timestamp)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://sor-data-custodia/tb_contratos_mainframe'
TBLPROPERTIES (
  'parquet.compress'='SNAPPY', 
  'transient_lastDdlTime'='1764366288')
```

#### Tabela SoT (contratos tratados com versão histórica)

``` sql
CREATE TABLE sot.tb_contratos_iceberg (
  contract_id string,
  produto string,
  status string,
  created_at timestamp,
  updated_at timestamp,
  valor_total double,
  valor_juros double,
  valor_iof double,
  parcelas_totais int,
  parcelas_quitadas int,
  id_cliente string,
  nome string)
PARTITIONED BY (day(`created_at`), bucket(100, `contract_id`))
LOCATION 's3://sot-data-custodia/tb_contratos_iceberg'
TBLPROPERTIES (
  'table_type'='iceberg',
  'write_compression'='snappy',
  'format'='parquet',
  'optimize_rewrite_delete_file_threshold'='10'
);
```

------------------------------------------------------------------------

### Estratégias de armazenamento histórico (SCD)

#### SCD1 com Iceberg (sobrescreve a linha atual)
Na expressão a seguir é feito insert na base de contratos da camada SoT quando um contrato não é encontrado na base origem.

``` sql
MERGE INTO catalog.sot.tb_contratos_iceberg  t
USING staging s
ON t.contract_id = s.contract_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```
#### SCD2 (mantém versões históricas)
Na expressão é utilizado o insert quando um contrato não está na base origem e também utilizado o update no caso em STATUS do contrato é alterado. Vale ressaltar que qualquer alteração além de STATUS pode ser considerada.

``` sql
MERGE INTO catalog.sot.tb_contratos_iceberg  t
USING staging s
ON t.contract_id = s.contract_id AND t.is_current = true
WHEN MATCHED AND t.status <> s.status THEN 
    UPDATE SET is_current = false
WHEN NOT MATCHED THEN 
    INSERT (t.contract_id,t.produto,t.status,t.created_at,t.updated_at,t.valor_total,t.valor_juros,t.valor_iof,t.parcelas_totais,t.parcelas_quitadas,t.id_cliente,t.nome)
    VALUES (s.contract_id,s.produto,s.status,s.created_at,s.updated_at,s.valor_total,s.valor_juros,s.valor_iof,s.parcelas_totais,s.parcelas_quitadas,s.id_cliente,s.nome);
```

------------------------------------------------------------------------

### Time Travel

#### Consultar snapshot específico:

``` sql
SELECT * FROM catalog.sot.tb_contratos_iceberg
FOR SYSTEM_TIME AS OF '2024-05-10T00:00:00';
```

#### Consultar lista de snapshots:

``` sql
SELECT * FROM catalog.sot.tb_contratos_iceberg.snapshots;
```

------------------------------------------------------------------------

### Configurações avançadas de otimização

- Compactação automática
- Otimização de metadados
- Limpeza de snapshots antigos

``` python
class IcebergQuery():

    def __init__(
        self,
        catalog_name,
        catalog_database,
        catalog_table,
        spark,
        older_than=None,
    ):
        self.catalog_name = catalog_name
        self.catalog_database = catalog_database
        self.catalog_table = catalog_table

        self.target_size = str(256 * 1024 * 1024)  # 256 MB
        self.max_file_group_size = str(10 * 1024 * 1024 * 1024)  # 10 GB
        self.older_than = older_than
        
        self.spark = spark

        """
        O parâmetro older_than = 'TIMESTAMP '2025-10-27 00:00:00.000''
        No comando expire_snapshots irá excluir snapshots criados antes
        do início do dia 27, ou seja, até 26/10/2025 23:59:59.999...
        """

        # Stored procedure do Iceberg para expiração de Snapshots
        # Remove snapshots antigos e libera arquivos não referenciados
        # retain_last => 10 — Garante manter pelo menos os dez snapshots mais recentes
        self.EXPIRE_SNAPSHOTS = f"""
            CALL {self.catalog_name}.system.expire_snapshots(
                table => '{self.catalog_database}.{self.catalog_table}',
                older_than => TIMESTAMP '{self.older_than}',
                retain_last => 10,
                max_concurrent_deletes => 16,
                stream_results => true
            )
        """

        # Stored procedure do Iceberg para exclusão de arquivos órfãos
        # Exclui arquivos que não estão mais referenciados por nenhum snapshot
        self.REMOVE_ORPHAN = f"""
            CALL {self.catalog_name}.system.remove_orphan_files(
                table => '{self.catalog_database}.{self.catalog_table}',
                older_than => TIMESTAMP '{self.older_than}',
                max_concurrent_deletes => 8,
                dry_run => false
            )
        """

        # Stored procedure do Iceberg para reescrita de arquivos de manifesto
        # Reorganiza os arquivos de manifesto para melhorar a performance de leitura e escrita
        self.REWRITE_MANIFESTS = f"""
            CALL {self.catalog_name}.system.rewrite_manifests(
                table => '{self.catalog_database}.{self.catalog_table}'
            )
        """

        # Stored procedure do Iceberg para compactação de arquivos
        self.REWRITE_DATA = f"""
            CALL {self.catalog_name}.system.rewrite_data_files(
                table => '{self.catalog_database}.{self.catalog_table}',
                options => map(
                    'target-file-size-bytes', {self.target_size},
                    'max-file-group-size-bytes', {self.max_file_group_size}
                )
            )
        """
```

------------------------------------------------------------------------

### Código PySpark para AWS Glue para MERGE Iceberg
O código a seguir mostra como uma tabela iceberg pode ser populada utilizando UPDATE e/ou INSERT. A principal finalidade deste Job é executar o trabalho e fazer harmonização na tabela Iceberg, cuidando de smallfiles e snapshots.

``` python
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from datacustodia.iceberg import Iceberg

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

tb_contratos_mainframe = glueContext.create_dynamic_frame.from_catalog(
    database='sor',
    table_name='tb_contratos_mainframe',
    transformation_ctx=f"tb_contratos_mainframe"
).toDF()

tb_contratos_mainframe.createOrReplaceTempView('tb_contratos_mainframe')

df = spark.sql('''
SELECT 
  *
FROM TB_CONTRATOS_MAINFRAME
''')

df.createOrReplaceTempView('stage')

spark.sql("""
MERGE INTO catalog.sot.tb_contratos_iceberg  t
USING staging s
ON t.contract_id = s.contract_id AND t.is_current = true
WHEN MATCHED AND t.status <> s.status THEN 
    UPDATE SET is_current = false
WHEN NOT MATCHED THEN 
    INSERT (t.contract_id,t.produto,t.status,t.created_at,t.updated_at,t.valor_total,t.valor_juros,t.valor_iof,t.parcelas_totais,t.parcelas_quitadas,t.id_cliente,t.nome)
    VALUES (s.contract_id,s.produto,s.status,s.created_at,s.updated_at,s.valor_total,s.valor_juros,s.valor_iof,s.parcelas_totais,s.parcelas_quitadas,s.id_cliente,s.nome);
""")

iceberg = IcebergQuery(
    catalog_database='sot',
    catalog_name='glue_catalog',
    spark=spark,
    catalog_table='tb_unindo_diferentes_fontes_iceberg'
)
```

### Código PySpark para Glue para fazer o controle de atualização de contratos
Esse código cria duas tabelas auxiliares Iceberg: uma para registrar a data em que cada contract_id apareceu pela primeira vez e outra para registrar a última atualização de cada contrato junto com o hash da sua linha. Em seguida, ele lê a tabela principal (tb_unindo_diferentes_fontes_iceberg) e gera um hash baseado em todas as colunas, permitindo detectar alterações no conteúdo. Depois disso, identifica quais contratos ainda não existem na tabela de primeira inserção e grava esses novos registros, marcando o timestamp da primeira aparição. Na sequência, compara o hash atual de cada contrato com o hash armazenado anteriormente na tabela de última atualização; sempre que o contrato não existe na tabela auxiliar ou quando o hash mudou, o código registra que houve uma atualização, salva o novo hash e grava a data dessa alteração. Por fim, ele grava novos contratos usando append e atualizações usando sobrescrita de partições, garantindo que o estado auxiliar se mantenha sincronizado com a tabela principal.
```python
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.sot.tb_aux_primeira_insercao (
    contract_id STRING,
    data_primeira_insercao TIMESTAMP
)
USING iceberg
PARTITIONED BY (contract_id)
LOCATION 's3://sor-data-custodia/tb_aux_primeira_insercao/'
TBLPROPERTIES (
    'format'='parquet',
    'write_compression'='snappy'
)        
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS glue_catalog.sot.tb_aux_ultima_atualizacao (
    contract_id STRING,
    data_ultima_atualizacao TIMESTAMP,
    hash_atual STRING
)
USING iceberg
PARTITIONED BY (contract_id)
LOCATION 's3://sor-data-custodia/tb_aux_ultima_atualizacao/'
TBLPROPERTIES (
    'format'='parquet',
    'write_compression'='snappy'
)
""")

df = spark.table("glue_catalog.sot.tb_unindo_diferentes_fontes_iceberg")

df_hash = df.withColumn(
    "hash_value",
    F.sha2(F.concat_ws("||", *df.columns), 256)
)

df_hash.cache()

df_primeira = spark.table("glue_catalog.sot.tb_aux_primeira_insercao")

novos_contratos = (
    df_hash
    .join(df_primeira, "contract_id", "left_anti")
    .select(
        "contract_id",
        F.current_timestamp().alias("data_primeira_insercao")
    )
)

if novos_contratos.count() > 0:
    novos_contratos.writeTo("glue_catalog.sot.tb_aux_primeira_insercao").append()


df_ultima = spark.table("glue_catalog.sot.tb_aux_ultima_atualizacao")

joined = (
    df_hash.alias("new")
    .join(df_ultima.alias("old"), "contract_id", "left")
    .select(
        "new.contract_id",
        F.col("new.hash_value").alias("hash_atual"),
        F.col("old.hash_atual").alias("hash_antigo")
    )
)

atualizados = (
    joined
    .filter((F.col("hash_antigo").isNull()) | (F.col("hash_antigo") != F.col("hash_atual")))
    .select(
        "contract_id",
        F.current_timestamp().alias("data_ultima_atualizacao"),
        "hash_atual"
    )
)

if atualizados.count() > 0:
    atualizados.writeTo("glue_catalog.sot.tb_aux_ultima_atualizacao").overwritePartitions()

spark.stop()
```

<p align="center">
    <img src="images/iceberg.gif" alt="Idempotência do job (idempotencia.gif)" />
</p>

------------------------------------------------------------------------

### Melhores práticas de produção

-   Preferir **MERGE Iceberg** ao invés de reescrever toda a tabela.
-   Manter **tabelas SoR separadas de SoT**.
-   Realizar **compaction semanal** ou junto Job principal.
-   Usar **partitioning baseado em coluna de atualização**.
-   Habilitar **data retention** para snapshots (evita S3 lotar).
-   Utilizar Bucketing particionamento por ID do contrato.
-   Preferir **integração com Glue Catalog**.

------------------------------------------------------------------------

### Conclusão

Apache Iceberg é a tecnologia ideal para armazenar histórico de
contratos, pois oferece: - alto desempenho, - governança, -
versionamento avançado, - particionamento evolutivo, - integração com os
principais engines, - queries analíticas eficientes via Athena e Spark.

Portanto, se um consumir ter interesse em consumir os dados de forma analítica a tabela Iceberg é a melhor opção. Também o mesmo pode utilizar as Tabelas Auxiliares para ajudar sua consulta na tabela SoR caso seja extremamente necessário.

------------------------------------------------------------------------

## Parte 5: Transformação para Cultura Data-Driven

### 1\. Liderar a Mudança de Cultura para Data-Driven em Times Acostumados ao Mainframe

A migração do Mainframe não é apenas uma mudança de tecnologia, é uma **mudança de *mindset***: do foco em **processamento transacional** para o foco em **produto de dados e valor analítico**.

#### Highlights de Estratégias e Ações:

-----

#### 1\. Foco no Valor, Não na Tecnologia (Quick Wins)

  * **Ação:** Identificar e entregar **"Quick Wins"** analíticos que eram impossíveis ou muito caros no Mainframe.
      * **Exemplo:** Criar um **dashboard de frescor de dados em tempo quase real** no Power BI (via Athena/S3) que mostre o saldo devedor consolidado. Isso demonstra **valor imediato** (Agilidade) sobre o esforço de migração.
  * **Estratégia:** Associar a nova plataforma (AWS) diretamente à **redução de *pain points*** e à **liberação de tempo** para o time.

-----

#### 2\. Upskilling e Empoderamento dos Times (Data Mesh Mindset)

  * **Ação:** Criar um **programa de *upskilling*** focado em PySpark, Glue, Data Quality e Data Modeling no S3.
      * **Foco:** Transformar o conhecimento do Mainframe (COBOL, JCL) em **conhecimento de Domínio de Negócio**. O engenheiro que entende a regra de negócio do Mainframe se torna o **Dono do Produto de Dados (Data Product Owner)** no Data Mesh.
  * **Estratégia:** Mudar a métrica de sucesso de "Código que roda" para "**Produto de Dados Consumido**" (uso e qualidade), reforçando o Data Mesh.

-----

#### 3\. Introdução de Governança e Qualidade (Glue Data Quality)

  * **Ação:** Tornar o **Glue Data Quality (DQ)** obrigatório em **100%** dos *pipelines* do novo ambiente.
      * **Foco:** Mostrar que a agilidade na nuvem vem com **mais controle e confiança** no dado. Usar as métricas de DQ (ex: taxa de completude) como um KPI de sucesso do time.
  * **Estratégia:** **Visualizar a Qualidade:** Criar um *Painel de Confiança dos Dados* (Data Trust Score) visível para todos os times, demonstrando o **ganho de confiabilidade** em relação ao ambiente antigo, onde a qualidade era opaca.

-----

#### 4\. Comunidade e Comunicação

  * **Ação:** Criar um **fórum/guilda de Data Mesh** (*Community of Practice*) para compartilhar padrões, desafios e soluções na AWS.
      * **Foco:** Incentivar os times a se ajudarem e a padronizar, quebrando silos e tornando a **mudança orgânica**.
  * **Estratégia:** **Celebrar o Desligamento do Mainframe (Decomissionamento):** Cada subsistema migrado e desativado deve ser celebrado como uma **vitória de negócio**, não apenas técnica, reforçando o impacto dos OKRs.

----

#### 5\. Data Champions
* **Ação**: Criar trilhas de conhecimento para possibilitando a capacitação dos times de mainframe e times de moderno.
  * **Foco:** Nivelar o conhecimento e incentivar o uso de tecnologias modernas