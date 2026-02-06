import logging
import json
import base64
import hmac
import hashlib
import jwt
from airflow import DAG
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
from airflow.decorators import task

from Importador.util.minio_utils import MinioUtils

from Importador.operators.edital_operator import EditalOperator
from Importador.operators.projeto_operator import ProjetoOperator
from Importador.operators.bolsistas_operator import BolsistasOperator
from Importador.plugins.callbacks.alerts import alerta_de_falha


@task(task_id="gerar_token_jwt")
def _gerar_jwt():
    def base64url_encode(data):
        if isinstance(data, str):
            data = data.encode('utf-8')
        return base64.urlsafe_b64encode(data).decode('utf-8').rstrip('=')

    try:
        b64_key = Variable.get("JWT_PRIVATE_KEY")
        secret_bytes = base64.b64decode(b64_key)
    except KeyError:
        raise Exception("A variável 'JWT_PRIVATE_KEY' não foi encontrada!")
    except Exception as e:
        raise ValueError(f"Erro ao decodificar chave: {e}")

    logging.info("Iniciando construção manual do JWT (Bypass PyJWT)...")

    header = {
        "alg": "HS256",
        "typ": "JWT"
    }

    payload = {
        "Id": "915d4a4d-8bad-42b0-9e8b-bc6353845cf7",
        "unique_name": "ImportacaoEditalJobUser",
        "email": "",
        "role": "ADMIN",
        "Cpf": "12345678901",
        "nbf": int(datetime.now(timezone.utc).timestamp()), 
        "iat": int(datetime.now(timezone.utc).timestamp()),
        "exp": int((datetime.now(timezone.utc) + timedelta(hours=1)).timestamp())
    }

    encoded_header = base64url_encode(json.dumps(header, separators=(',', ':')))
    encoded_payload = base64url_encode(json.dumps(payload, separators=(',', ':')))

    signing_input = f"{encoded_header}.{encoded_payload}".encode('utf-8')
    
    signature = hmac.new(
        secret_bytes,     
        signing_input,    
        hashlib.sha256    
    ).digest()

    encoded_signature = base64url_encode(signature)

    token = f"{encoded_header}.{encoded_payload}.{encoded_signature}"

    token = jwt.encode(payload, b64_key, algorithm="HS256")
    
    logging.info("Token gerado manualmente com sucesso.")
    return token

@task(task_id="salvar_token_na_variable")
def _salvar_token_na_variable(token_gerado: str):
    logging.info("Salvando token na Variable 'AUTH_TOKEN'...")
    token_completo = "Bearer " + token_gerado
    Variable.set("AUTH_TOKEN", token_completo)
    logging.info("Token salvo com sucesso na Variable 'AUTH_TOKEN'.")

@task(task_id="mover_arquivos_processados")
def _mover_arquivos_processados(**context):
    conf = context['dag_run'].conf or {}
    
    edital_num = conf.get('edital_num')
    pasta = conf.get('pasta')

    if not edital_num or not pasta:
        logging.error("ERRO: Parâmetros 'edital_num' e 'pasta' são obrigatórios via API.")
        raise ValueError("Parâmetros ausentes")

    logging.info(f"Movendo arquivos do Edital {edital_num} da pasta {pasta}...")

    minio_endpoint = Variable.get("MINIO_ENDPOINT")
    minio_access_key = Variable.get("MINIO_ACCESS_KEY")
    minio_secret_key = Variable.get("MINIO_SECRET_KEY")
    minio_client = MinioUtils(minio_endpoint, minio_access_key, minio_secret_key)

    bucket = 'importacao'
    base_path = 'JANUARY'
    processed_path = 'PROCESSADOS'

    prefix = f"{base_path}/{pasta}/"
    objects = list(minio_client.client.list_objects(bucket, prefix=prefix, recursive=True))

    arquivos_movidos = 0
    for obj in objects:
        if str(edital_num) in obj.object_name:
            new_key = obj.object_name.replace(base_path, processed_path, 1)
            source = CopySource(bucket, obj.object_name)
            
            minio_client.client.copy_object(bucket, new_key, source)
            minio_client.client.remove_object(bucket, obj.object_name)
            logging.info(f"Movido: {obj.object_name} -> {new_key}")
            arquivos_movidos += 1
            
    logging.info(f"Total de arquivos movidos: {arquivos_movidos}")
            
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback' : alerta_de_falha,
}

with DAG(
    dag_id='SigFapes2Conecta',
    default_args=default_args,
    description='DAG de Importação de Dados do Appsmith para o Conecta',
    start_date=datetime(2025, 4, 2),
    schedule_interval=None,
    max_active_runs=5,
    catchup=False,
    tags=['conecta', 'sigfapes', 'api']
) as dag:

    token = _gerar_jwt()
    salvar_token = _salvar_token_na_variable(token)

    edital_op = EditalOperator(
        task_id='Edital',
        use_validated_input=True,
        minio_bucket='importacao',
        minio_file_name="JANUARY/{{ dag_run.conf['pasta'] }}/{{ dag_run.conf['edital_num'] }}_edital.jsonl",
        edital_num="{{ dag_run.conf['edital_num'] }}"
    )

    projeto_op = ProjetoOperator(
        task_id='Projeto',
        use_validated_input=True,
        minio_bucket='importacao',
        minio_file_name="JANUARY/{{ dag_run.conf['pasta'] }}/{{ dag_run.conf['edital_num'] }}_projeto.jsonl"
    )

    bolsistas_op = BolsistasOperator(
        task_id='Bolsistas',
        use_validated_input=True,
        minio_bucket='importacao',
        minio_file_name="JANUARY/{{ dag_run.conf['pasta'] }}/{{ dag_run.conf['edital_num'] }}_bolsistas.jsonl"
    )

    # mover = _mover_arquivos_processados()

    token >> salvar_token >> edital_op >> projeto_op >> bolsistas_op
    
