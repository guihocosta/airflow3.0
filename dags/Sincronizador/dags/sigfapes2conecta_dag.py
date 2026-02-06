from airflow import DAG
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
from Sincronizador.operators.edital_operator import EditalOperator
from Sincronizador.operators.projeto_operator import ProjetoOperator
from Sincronizador.operators.bolsistas_operator import BolsistasOperator
from Sincronizador.plugins.callbacks.alerts import alerta_de_falha
import jwt
import base64
from airflow.decorators import task

@task(task_id="gerar_token_jwt")
def _gerar_jwt():
    import json
    import base64
    import hmac
    import hashlib
    from datetime import datetime, timedelta, timezone
    from airflow.models import Variable

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

    print("Iniciando construção manual do JWT (Bypass PyJWT)...")

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
    
    print("Token gerado manualmente com sucesso.")
    return token

@task(task_id="salvar_token_na_variable")
def _salvar_token_na_variable(token_gerado: str):
    print("Salvando token na Variable 'AUTH_TOKEN'...")
    token_completo = "Bearer " + token_gerado
    Variable.set("AUTH_TOKEN", token_completo)
    print("Salvo com sucesso.")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback' : alerta_de_falha,
}

with DAG(
    dag_id='Sincronizador',
    default_args=default_args,
    description='DAG de Sincronização de Dados do SigFapes para o Conecta',
    start_date=datetime(2025, 4, 2),
        
    catchup=False,
    tags=['conecta', 'sigfapes']
) as dag:

    gerar_token_jwt_task = _gerar_jwt()
    salvar_token_task = _salvar_token_na_variable(gerar_token_jwt_task)

    edital_num = "1015"
    edital = EditalOperator(
    task_id='Edital',
    use_validated_input=False,
    minio_bucket='importacao',
    minio_file_name=f'DECEMBER-2025/PROGRAMAS/{edital_num}_edital.jsonl'
    )

    projeto = ProjetoOperator(
        task_id='Projeto',
        use_validated_input=False,
        minio_bucket='importacao',
        minio_file_name=f'DECEMBER-2025/PROGRAMAS/{edital_num}_projeto.jsonl'
    )

    bolsistas = BolsistasOperator(
        task_id='Bolsistas',
        use_validated_input=False,
        minio_bucket='importacao',
        minio_file_name=f'DECEMBER-2025/EDITAIS/{edital_num}_bolsistas.jsonl'
    )

    gerar_token_jwt_task >> salvar_token_task >> edital >> projeto >> bolsistas

    
