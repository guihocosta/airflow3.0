import json
from airflow.providers.postgres.hooks.postgres import PostgresHook
from ..util.postgre import PostgresQL
from airflow.utils.decorators import apply_defaults
from airflow.models.baseoperator import BaseOperator
import requests
from airflow.models import Variable
from ..util.minio_utils import MinioUtils
from ..util.sqlserver import SqlServer
from io import BytesIO

class Fapes2ConectaOperator(BaseOperator):
    template_fields = ['minio_file_name', 'use_validated_input']

    def __init__(self, funcao: str | None = None, *args, **kwargs):

        raw_input = kwargs.pop("use_validated_input", False)

        if isinstance(raw_input, str):
            self.use_validated_input = raw_input.lower() == 'true'
        else:
            self.use_validated_input = bool(raw_input)
        self.log.info(self.use_validated_input)
        self.minio_bucket: str | None = kwargs.pop("minio_bucket", None)
        self.minio_file_name: str | None = kwargs.pop("minio_file_name", None)

        super().__init__(*args, **kwargs)

        self.funcao = funcao
        self.with_dependency = False

        self.login = None
        self.senha = None
        self.conectaToken = None
        self.conectaUrl = None
        self.mssql = None

        self.parentConectaId = None
        self.parentSigfapesId = None
        self.new_ids: dict = {}
        self.data_minio: list = []

        self.items_all_data = {}

        self.task_ids = None
        self.key = None
        self.attrname = None
        self.selected_id = None
        self.parent_id = None
        self.selected_status = None
        self.fapesToken = None

        self.area_tecnica = None

    def _post(self, url, payload, headers=None, verify=False):

        default_headers = {
        'Content-Type': 'application/json',
        'accept': 'application/json'
        }

        final_headers = {**default_headers, **(headers or {})}

        response = requests.post(url, json=payload, headers=final_headers, verify=verify)
        
        if response.status_code > 299:
            error_message = f"Erro CRÍTICO ao enviar dados para API ({url}): {response.text}"
            self.log.error(error_message)
            raise requests.HTTPError(error_message)

        try:
            return response.json()
        
        except ValueError:
            self.log.error("Resposta da API não é JSON: %s", response.text)
            return {
                "statusCode": 404,
                "errors": response.text,
                "message": response.text,
            }
        
    def __autencicacao_fapes(self):
        url = 'https://servicos.fapes.es.gov.br/webServicesSig/auth.php'
        
        payload = {
            'username': self.login,
            'password': self.senha
        }

        response = self._post(url, payload=payload, verify=False)

        if response is None:
            raise PermissionError('Falha na autenticação com o Sigfapes')
            
        self.fapesToken = response['token']

    def __process_children(self, context):
        ids = self.ti.xcom_pull(task_ids=self.task_ids, key=self.key)

        self.log.info('%s a serem percorridos. ', len(ids))
        elements = []

        if self.use_validated_input:
            self.items_all_data = self._import_minio(self.minio_bucket, self.minio_file_name)

        for conecta_id, (sigfapes_id, area_tecnica) in ids.items():

            self.area_tecnica = area_tecnica

            # Extract
            data = self._retrive(attrname=self.attrname,attrvalue=sigfapes_id)
            elements.extend(data)

            elements_id = self._select(data,self.selected_id)

            dict_sigfapes = {
                item[self.selected_id]: {
                    "status": item[self.selected_status],
                    "raw": item
                    }
                for item in data
            }

            to_update = self._filter_to_update(dict_sigfapes, conecta_id)

            if len(to_update) != 0:
                elements_update_transform = self._transform_update(to_update)
                self._sink_update(elements_update_transform)
            
            # Novos Ids
            new_ids = self._filter(elements_id)

            if len(new_ids) != 0:
                self.parentSigfapesId, self.parentConectaId = sigfapes_id, conecta_id
                
                new_elements = self.__get_elements_by_id(new_ids, data, self.selected_id)
                
                # Importar as Dependencias, se houver
                if self.with_dependency:
                    self._import_dependency(new_elements)

                # Transform
                elements_transform = self._transform(new_elements)
                
                # Load
                self._sink(elements_transform, new_elements)

        self.ti.xcom_push(key=self.funcao, value=self.new_ids)
        return elements
    
    def __process_without_children(self, context):

        #Extract
        data = self._retrive()
        elements_id = self._select(data,'edital_id')
        new_ids = self._filter(elements_id)

        if len(new_ids) == 0:
            self.log.info("Nenhum edital para ser importado.")
            self.ti.xcom_push(key='editais', value=self.new_ids)
            return

        new_elements = self.__get_elements_by_id(new_ids, data, 'edital_id')

        # Transform
        elements_transform = self._transform(new_elements)

        # Load
        self._sink(elements_transform, new_elements)
        
        self.ti.xcom_push(key='editais', value=self.new_ids)
        return new_elements
        
    def _process(self, context):
        if self.task_ids and self.key:
           return self.__process_children(context)
        else: 
           return self.__process_without_children(context)
      
    def execute(self, context):
        self.ti = context.get('ti')
        self.login = Variable.get("FAPES_USER")
        self.senha = Variable.get("FAPES_SENHA")
        self.conectaToken = Variable.get("AUTH_TOKEN")
        self.conectaUrl = Variable.get("CONECTAFAPES_URL")
        self.mssql = SqlServer(conn_id="sql_server_importacao")
        self.__autencicacao_fapes()
        self._process(context=context)

    def _retrive(self, attrname=None, attrvalue=None):

        if self.use_validated_input:

            if self.parent_id:
                return [item for item in self.items_all_data if str(item[self.parent_id]) == str(attrvalue)]

            return self.items_all_data
        
        url =  "https://servicos.fapes.es.gov.br/webServicesSig/consulta.php"
        payload = {
            'token': self.fapesToken,
            'funcao': self.funcao,
        }
        if attrname and attrvalue:
            payload[attrname] = attrvalue
            
        data =  self._post(url,payload, verify=False)
        
        # Check if 'data' key exists in the response
        if 'data' not in data:
            raise KeyError("A resposta não contém a chave 'data'")
        
        return data['data']
   
    def _select(self, data, key=str):
        get_edital_ids = lambda data_list: [item[key] for item in data_list]
        # Using
        return get_edital_ids(data)

    def __get_elements_by_id(self, id, data, key=str):

        """
        Retorna os novos elementos filtrados pelo ID
        """

        elements = [elem for elem in data if elem[key] in id]
        return elements
    
    def _sink_importacao(self, table_sqlserver, values_sqlserver, columns_sqlserver):
        self.mssql.bulk_insert(table=table_sqlserver,columns=columns_sqlserver,values=values_sqlserver)

    def _sink_importacao_update(self, table, id, columns, values):
        self.mssql.bulk_update(
            table=table,
            id_column=id,
            update_columns=columns,
            data=values)

    def _sink_minio(self, data, bucket_name, prefix):
        
        minio_uploader = MinioUtils(
            endpoint=Variable.get("MINIO_URL"),
            access_key=Variable.get("MINIO_ACCESS_KEY"),
            secret_key=Variable.get("MINIO_SECRET_KEY") 
        )
        
        # Faz o upload
        minio_uploader.upload(data, bucket_name, prefix)
    
    def _import_minio(self, bucket_name, prefix_or_file):

        client = MinioUtils(
            Variable.get("MINIO_URL"),
            access_key=Variable.get("MINIO_ACCESS_KEY"),
            secret_key=Variable.get("MINIO_SECRET_KEY"),
        )

        if prefix_or_file.endswith(".json") or prefix_or_file.endswith(".parquet"):
            file_name = prefix_or_file
        else:
            file_name = client.latest_file_with_prefix(bucket_name, prefix_or_file)

        return client.get(bucket_name, file_name)

    def _sink_conecta(self, data, endpoint):

        headers = {"Authorization": self.conectaToken}

        resp = self._post(self.conectaUrl + endpoint,
                            payload=data,
                            headers=headers,
                            verify=False)
        

        entity_id_str = resp['uri'].split('/')[-1]
        return True, str(entity_id_str)

    def _get(self, url, headers={}, verify=False):

        response = requests.get(url, 
        headers=headers, verify=verify)

        if response.status_code > 299:
            error_message = f"Erro CRÍTICO ao enviar dados para API ({url}): {response.text}"
            self.log.error(error_message)

            raise requests.HTTPError(error_message)
        
        return response.json()
    
    def get_conecta(self, endpoint):
        headers = {"Authorization": self.conectaToken}

        resp = self._get(url = self.conectaUrl + endpoint,
                            headers=headers,
                            verify=False)
        
        return resp


    def _put(self, url, payload, headers=None, params=None, verify=False):

        default_headers = {
            "Content-Type": "application/json",
            "accept": "application/json"
        }

        final_headers = {**default_headers, **(headers or {})}

        response = requests.put(
            url,
            headers=final_headers,
            params=params,
            json=payload,
            verify=verify
        )

        if response.status_code > 299:
            error_message = f"Erro CRÍTICO ao enviar dados para API ({url}): {response.text}"
            self.log.error(error_message)

            raise requests.HTTPError(error_message)
        
        return json.loads(response.text)

    def _sink_conecta_update(self, data, endpoint):
        headers = {"Authorization": self.conectaToken}

        resp = self._put(self.conectaUrl + endpoint,
                            payload=data,
                            headers=headers,
                            verify=False)
        
        return resp
        
    def _transform(self, data):
        raise NotImplementedError()
    
    def _import_dependency(self, data):
        raise NotImplementedError()

    def _filter_to_update(self, data, parent_id):
        raise NotImplementedError()

    def _filter(self,data):
        raise NotImplementedError()

    def _sink (self, data_transform, data):
        raise NotImplementedError()

    def _transform_update(self, data_update):
        raise NotImplementedError()
    
    def _sink_update(self, data):
        raise NotImplementedError()
    
    def _sink_error(self, data):
        raise NotImplementedError()