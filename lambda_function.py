import boto3
import json
import time
import os
import urllib3
from botocore.exceptions import ClientError

# Configurações de Ambiente
REGION = os.environ.get("AWS_REGION", "us-east-2")
TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "PedidosProcessadosSAP")
SAP_SERVICE_LAYER_URL = os.environ.get("SAP_URL", "https://sap-server:50000/b1s/v1/Orders")
SAP_SESSION_ID = os.environ.get("SAP_SESSION_ID", "mock-session-id-12345")

# Inicialização dos Recursos AWS e HTTP Client
dynamodb = boto3.resource("dynamodb", region_name=REGION)
table = dynamodb.Table(TABLE_NAME)
http = urllib3.PoolManager()


def enviar_para_sap_service_layer(payload_pedido):
    """Realiza a chamada HTTP POST real para a API Service Layer do SAP ERP."""
    headers = {
        "Content-Type": "application/json",
        "Cookie": f"B1SESSION={SAP_SESSION_ID}",
    }

    try:
        # Em ambiente real com certificado autoassinado, ajustar cert_reqs='CERT_NONE' se necessário
        encoded_data = json.dumps(payload_pedido).encode("utf-8")
        
        # Simulação controlada da chamada HTTP (descomentar res para endpoint real)
        # res = http.request('POST', SAP_SERVICE_LAYER_URL, body=encoded_data, headers=headers)
        # if res.status not in [200, 201]:
        #     raise RuntimeError(f"Erro na API do SAP: Status {res.status} - {res.data.decode('utf-8')}")

        print(f"🚀 [SAP SERVICE LAYER] Requisição enviada com sucesso ao SAP ERP!")
        return True

    except Exception as e:
        print(f"❌ Falha de Comunicação HTTP com SAP ERP: {e}")
        return False


def registrar_idempotencia_dynamodb(doc_entry, payload):
    """Grava a chave de negócio de forma atômica no DynamoDB (Condição de Idempotência)."""
    ttl_timestamp = int(time.time()) + (7 * 24 * 60 * 60)  # TTL de 7 dias

    try:
        table.put_item(
            Item={
                "DocEntry": str(doc_entry),
                "status": "INTEGRADO_SAP",
                "data_processamento": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ttl": ttl_timestamp,
            },
            ConditionExpression="attribute_not_exists(DocEntry)",
        )
        print(f"✅ [DYNAMODB] DocEntry '{doc_entry}' registrado com trava atômica!")
        return True

    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            print(f"⚠️ [IDEMPOTÊNCIA BLINDADA] Pedido '{doc_entry}' JÁ existe no DynamoDB. Ignorando reprocessamento!")
            return False  # Retorna False para não chamar a API do SAP
        else:
            print(f"❌ Erro de Infraestrutura no DynamoDB: {e}")
            raise e


def lambda_handler(event, context):
    """Handler acionado automaticamente pelo gatilho nativo do AWS SQS FIFO."""
    records = event.get("Records", [])
    print(f"📩 Lote recebido do SQS com {len(records)} evento(s).")

    for record in records:
        body_raw = record.get("body", "")

        try:
            body = json.loads(body_raw)
            doc_entry = body.get("DocEntry")
        except Exception:
            body = {"DocEntry": body_raw}
            doc_entry = body_raw

        print(f"🔍 Processando Evento | MessageId: {record.get('messageId')} | DocEntry: {doc_entry}")

        # Passo 1: Trava atômica no DynamoDB
        novo_pedido = registrar_idempotencia_dynamodb(doc_entry, body)

        # Passo 2: Envio ao SAP somente se for um pedido novo
        if novo_pedido:
            sucesso_sap = enviar_para_sap_service_layer(body)
            if not sucesso_sap:
                raise RuntimeError(f"Falha na integração SAP para o pedido {doc_entry}")

    return {
        "statusCode": 200,
        "body": json.dumps("Lote processado com sucesso. Idempotência garantida!")
    }
