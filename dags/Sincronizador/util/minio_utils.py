from minio import Minio
from minio.error import S3Error
from datetime import datetime
from io import BytesIO
import json
import pandas as pd
import numpy as np

class MinioUtils:
    def __init__(self, endpoint, access_key, secret_key, secure=True):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.secure = secure

        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )

    def _ensure_bucket(self, bucket_name):
        if not self.client.bucket_exists(bucket_name):
            self.client.make_bucket(bucket_name)

    def _normalize_parquet_output(self, obj):
        if isinstance(obj, dict):
            return {k: self._normalize_parquet_output(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._normalize_parquet_output(item) for item in obj]
        elif isinstance(obj, np.ndarray):
            return self._normalize_parquet_output(obj.tolist())
        else:
            return obj
        
    def upload(self, data, bucket_name, prefix):
        """
        Serializa e envia `data` para o MinIO como JSON.
        """
        self._ensure_bucket(bucket_name)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_name = f"{prefix}_{timestamp}.json"

        json_data = json.dumps(data, indent=4, default=str).encode("utf-8")
        data_bytes = BytesIO(json_data)

        self.client.put_object(
            bucket_name=bucket_name,
            object_name=object_name,
            data=data_bytes,
            length=len(json_data),
            content_type="application/json"
        )

        return object_name 

    def get(self, bucket_name, file_name):
        """
        Baixa e retorna o conteúdo de um arquivo salvo no MinIO.
        Retorna um dict (JSON) se for .json ou .parquet.
        """
        response = self.client.get_object(bucket_name, file_name)
        content = response.read()

        if file_name.endswith('.json'):
            return json.load(BytesIO(content))
        
        if file_name.endswith('.jsonl'):
            return [json.loads(line) for line in BytesIO(content).read().decode('utf-8').splitlines()]

        if file_name.endswith('.parquet'):
            df = pd.read_parquet(BytesIO(content))
            dict_data = df.to_dict(orient='records')
            return self._normalize_parquet_output(dict_data)
        
        raise ValueError(f"Formato de arquivo não suportado: {file_name}")

    def latest_file_with_prefix(self, bucket_name, prefix):
        """
        Retorna o nome do arquivo mais recentemente modificado com o prefixo especificado.
        Usa a data real de modificação no MinIO, não apenas o nome do arquivo.
        """
        try:
            objects = [
                obj for obj in self.client.list_objects(bucket_name, recursive=True)
                if obj.object_name.startswith(prefix)
            ]

            if not objects:
                raise FileNotFoundError(f"Nenhum arquivo encontrado com prefixo '{prefix}' no bucket '{bucket_name}'.")

            # Ordena por last_modified (datetime), do mais recente para o mais antigo
            latest = max(objects, key=lambda obj: obj.last_modified)

            return latest.object_name

        except S3Error as e:
            raise RuntimeError(f"Erro ao acessar o MinIO: {e}")

