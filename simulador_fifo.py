import boto3
import json
import uuid
import time
from datetime import datetime

# Configuração do cliente SQS usando o perfil padrão de credenciais da máquina
REGION = "us-east-2"
sqs = boto3.client("sqs", region_name=REGION)

# URL da sua fila FIFO no SQS
QUEUE_URL = "https://sqs.us-east-2.amazonaws.com/988661375338/sap-orders-queue.fifo"

def enviar_pedido_fifo(doc_entry, dedup_id, group_id="grupo-pedidos-sap"):
    payload = {
        "DocEntry": doc_entry,
        "CardCode": "C30000",
        "DocDate": datetime.now().strftime("%Y-%m-%d"),
        "DocTotal": 2500.00,
        "Comments": "Pedido via Magento - Teste de Idempotencia FIFO",
        "DocumentLines": [
            {
                "ItemCode": "B00002",
                "Quantity": 1,
                "Price": 2500.00,
                "TaxCode": "IVA_STD"
            }
        ]
    }
    
    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        MessageBody=json.dumps(payload),
        MessageGroupId=f"grupo-{uuid.uuid4()}",                  # Obrigatório em SQS FIFO (garante a ordem por grupo)
        MessageDeduplicationId=dedup_id,          # Obrigatório em SQS FIFO (chave de deduplicação nativa)
        MessageAttributes={
            'Source': {
                'DataType': 'String',
                'StringValue': 'EcommerceWebhook'
            }
        }
    )
    
    print(f"[+] Pedido Enviado! DocEntry: {doc_entry} | DedupId: {dedup_id} | MessageId: {response['MessageId']}")

if __name__ == "__main__":
    pedidoid_unico = str(uuid.uuid4())[:8]
    dedup_chave_1 = str(uuid.uuid4())
    
    print("=== TESTE 1: Deduplicação Nativa da AWS SQS FIFO ===")
    print("Enviando primeira mensagem...")
    enviar_pedido_fifo(doc_entry=pedidoid_unico, dedup_id=dedup_chave_1)
    
    print("Enviando mensagem DUPLICADA com o mesmo MessageDeduplicationId...")
    enviar_pedido_fifo(doc_entry=pedidoid_unico, dedup_id=dedup_chave_1)
    
    time.sleep(2)
    
    print("\n=== TESTE 2: Idempotência na Aplicação (Lambda) ===")
    dedup_chave_2 = str(uuid.uuid4())
    print("Enviando mensagem com NOVO MessageDeduplicationId, mas com MESMO DocEntry de negócio...")
    enviar_pedido_fifo(doc_entry=pedidoid_unico, dedup_id=dedup_chave_2)
