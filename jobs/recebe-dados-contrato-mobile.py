import json
import base64
import boto3
from decimal import Decimal

firehose = boto3.client("firehose")

DELIVERY_STREAM_NAME = "persiste-dados-mobile-s3"


def _decimal_to_float(obj):
    """
    Converte Decimal do DynamoDB para float/int para permitir serialização JSON.
    """
    if isinstance(obj, list):
        return [_decimal_to_float(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: _decimal_to_float(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        # tenta converter para int se não tiver casas decimais
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    return obj


def lambda_handler(event, context):
    records_to_firehose = []

    for record in event["Records"]:

        # ---- Identifica o tipo de operação ----
        event_name = record["eventName"]  # INSERT, MODIFY, REMOVE

        # ---- Captura o conteúdo do registro ----
        if event_name in ("INSERT", "MODIFY"):
            new_image = record["dynamodb"].get("NewImage", {})
            item = {k: _convert_dynamo_value(v) for k, v in new_image.items()}
        else:
            continue

        # ---- Converte Decimal → tipos nativos ----
        item = _decimal_to_float(item)

        # ---- Adiciona metadados ----
        item["dynamodb_event"] = event_name
        item["event_timestamp"] = record["dynamodb"]["ApproximateCreationDateTime"]

        # ---- Serializa para enviar ao Firehose ----
        json_line = json.dumps(item) + "\n"

        records_to_firehose.append({
            "Data": json_line.encode("utf-8")
        })

    # ---- Envio em lote para o Firehose ----
    if records_to_firehose:
        response = firehose.put_record_batch(
            DeliveryStreamName=DELIVERY_STREAM_NAME,
            Records=records_to_firehose
        )
        print("Firehose response:", response)

    return {"status": "ok", "sent": len(records_to_firehose)}


# ------------------------------------------------------
# Conversor genérico de formato DynamoDB → Python
# ------------------------------------------------------

def _convert_dynamo_value(value):
    """
    Converte os tipos do DynamoDB Streams (formato JSON)
    para tipos Python convencionais.
    """
    if "S" in value:
        return value["S"]
    if "N" in value:
        return Decimal(value["N"])
    if "BOOL" in value:
        return value["BOOL"]
    if "NULL" in value:
        return None
    if "L" in value:
        return [_convert_dynamo_value(v) for v in value["L"]]
    if "M" in value:
        return {k: _convert_dynamo_value(v) for k, v in value["M"].items()}
    return None
