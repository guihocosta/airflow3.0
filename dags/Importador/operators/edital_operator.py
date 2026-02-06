from concurrent.futures import ThreadPoolExecutor
from dateutil import parser
from datetime import timezone
from .fapes2conecta_operator import Fapes2ConectaOperator
import uuid
import datetime

class EditalOperator(Fapes2ConectaOperator):

    template_fields = ["edital_num", "minio_file_name", "use_validated_input"]

    TABLE_EDITAL_IMPORT_SQLSERVER = 'dbo.EditalImport'

    COLUMNS_EDITAL_IMPORT_SQLSERVER = [
        "Id", "IdSigfapes", "Nome", "EditalConectaId", "EhEditalImportado",
        "PossuiProjetosImportados", "DateCreated", "DateUpdated", "DateDeleted", "AreaTecnica"
    ]

    def __init__(self, edital_num=None, *args, **kwargs):
        super().__init__(*args, funcao="editais", **kwargs)
        self.edital_num = edital_num

    def __format_date_to_iso(self, date_str):
        dt_obj = parser.parse(date_str)
        dt_utc = dt_obj.astimezone(timezone.utc)
        return dt_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    
    def _filter(self, data):
        """
        Filtra IDs de editais, retornando apenas aqueles que são novos ou foram modificados.
        """
        data = [self.edital_num] if self.edital_num else []
        self.log.info(data)

        existing_ids = self.mssql.fetch_existing_ids(
            table=self.TABLE_EDITAL_IMPORT_SQLSERVER,
            column="IdSigfapes",
            ids=data
        )

        novos_ids = [id for id in data if str(id) not in existing_ids]
        return data

    def _transform(self, data):
        """Transforma os dados brutos do edital para o formato esperado pelo Conecta."""
        data_transform = []

        for edital in data:
            try:
                data_transform.append({
                    "nome": edital.get("edital_nome"),
                    "dataCriacao": self.__format_date_to_iso(edital.get("edital_data_cadastro")),
                    "idSigfapes": int(edital.get("edital_id")),
                    "AreaTecnica":edital.get("area_tecnica", None)
                })
            except Exception as e:
                self.log.error("Erro ao Transformar Edital: %s: %s", edital.get("edital_id"), str(e))
                raise e
        
        return data_transform
    
    def __prepare_record_sql(self, data, conecta_id):
        """Prepara uma tupla de dados para inserção na tabela de sucesso."""
        return (
            uuid.uuid4(),
            data["idSigfapes"],
            data["nome"],
            conecta_id,
            True, 
            False,  
            datetime.datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
            None,
            None,
            data["AreaTecnica"]
        )
    
    def _sink(self, data_transform, data):
        data_importacao_sqlserver = []

        def process(i):
            request = data_transform[i].copy()
            del request["AreaTecnica"]

            _, conecta_result = self._sink_conecta(request, 'edital')

            self.new_ids[str(conecta_result)] = (str(data_transform[i]['idSigfapes']), data_transform[i]["AreaTecnica"])
            data_importacao_sqlserver.append(self.__prepare_record_sql(data_transform[i], conecta_result))

        with ThreadPoolExecutor(max_workers=10) as executor:
            list(executor.map(process, range(len(data_transform))))

        if len(data_importacao_sqlserver) != 0:
            self.log.info("%s Editais importados com sucesso.", len(data_importacao_sqlserver))
            self._sink_importacao(self.TABLE_EDITAL_IMPORT_SQLSERVER,
                                  data_importacao_sqlserver,
                                  self.COLUMNS_EDITAL_IMPORT_SQLSERVER)

    def execute(self, context):
        if self.mssql is None:
            from ..util.sqlserver import SqlServer
            self.mssql = SqlServer(conn_id="sql_server_importacao")

        return super().execute(context)

    def _retrive(self, attrname=None, attrvalue=None):
        if self.use_validated_input:
            return self._import_minio(self.minio_bucket, self.minio_file_name)
        return super()._retrive(attrname, attrvalue)
