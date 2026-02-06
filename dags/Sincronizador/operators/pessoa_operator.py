from datetime import timezone
from dateutil import parser
import re
from ..enums import EnumEstadoCivil, EnumSexo, EnumRegimeCasamento
import uuid 
import datetime
class PessoaOperator():

    # Constantes para nomes de tabelas e colunas
    TABLE_PESSOA_IMPORT = 'dbo.PessoaImport'
    COLUMNS_PESSOA_IMPORT = [
                "Id",
                "IdSigfapes",
                "Nome",
                "Cpf",
                "PessoaConectaId",
                "DateCreated",
                "DateUpdated",
                "DateDeleted"
            ]

    def __init__(self, operator):
        self.operator = operator

        self.pessoas_para_importar = []

        self.pessoas_importadas_list = self.operator.mssql.fetch_as_dicts(
            self.TABLE_PESSOA_IMPORT, ["IdSigfapes", "PessoaConectaId"]
        )

        self.pessoas_importadas = {
            p["IdSigfapes"]: p["PessoaConectaId"]
            for p in self.pessoas_importadas_list
        }

    def __format_date_to_iso(self, date_str: str) -> str:
        """Converte uma string de data para o formato ISO 8601 com timezone UTC."""

        # Remove offsets não convencionais do tipo -03:06:28
        date_str = re.sub(r'[-+]\d{2}:\d{2}:\d{2}$', '', date_str)

        dt_obj = parser.parse(date_str)
        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.replace(tzinfo=timezone.utc)

        dt_utc = dt_obj.astimezone(timezone.utc)
        return dt_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

    def _transform(self, data):

        try:
            return {
                "nome": data["pesquisador_nome"],
                "cpf": data["pesquisador_cpf"],
                "email": data["pesquisador_email"],
                "dataNascimento": self.__format_date_to_iso(data["pesquisador_nascimento"]),
                "nomeMae": data["pesquisador_mae"],
                "enumEstadoCivil": EnumEstadoCivil.SOLTEIRO.value,
                "regimeCasamento": EnumRegimeCasamento.NENHUM.value,
                "sexo": EnumSexo.MASCULINO.value if int(data["pesquisador_sexo"]) == 0 else EnumSexo.FEMININO.value,
                "idSigfapes": int(data["pesquisador_id"])
                }
        except Exception as e:
            self.operator.log.error(f"Erro de Transformação de Pessoa: {str(e)}")
            raise e

    def __prepare_sucess(self, data, conecta_id):
        return (
            uuid.uuid4(),
            data["idSigfapes"],
            data["nome"],
            data["cpf"],
            conecta_id,
            datetime.datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
            None,
            None
        )

    def execute(self, data):
        
        id_sigfapes = data.get("pesquisador_id")

        # Verifica se já foi importado
        if id_sigfapes in self.pessoas_importadas:
            return (self.pessoas_importadas[id_sigfapes], 1)

        # Transform
        payload = self._transform(data)

        # Load
        _, conecta_id = self.operator._sink_conecta(payload, 'pessoa')
        
        self.pessoas_importadas[id_sigfapes] = conecta_id

        pessoa_import = self.__prepare_sucess(payload, conecta_id)
        self.pessoas_para_importar.append(pessoa_import)
        
        return (conecta_id, 0)

    def sink_importacao(self):
                
    # Sink no Banco da Importação
        self.operator._sink_importacao(table_sqlserver=self.TABLE_PESSOA_IMPORT, columns_sqlserver=self.COLUMNS_PESSOA_IMPORT, values_sqlserver=self.pessoas_para_importar)
        self.pessoas_para_importar = []
        