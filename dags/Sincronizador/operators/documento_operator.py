import re
from datetime import timezone
from dateutil import parser
from ..enums import TipoDocumentoEnum

class DocumentoOperator():

    def __init__(self, operator):
        self.operator = operator
        self.documentos_processados = set()

    def __format_date_to_iso(self, date_str):
        """Converte uma string de data para o formato ISO 8601 com timezone UTC."""
        dt_obj = parser.parse(date_str)
        dt_utc = dt_obj.astimezone(timezone.utc)
        return dt_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

    def _check_exists(self, pessoa_conecta_id):
        """
        Verifica se os dados de Documento já existem.
        """
        sql = "SELECT TOP 1 1 FROM dbo.Documento WHERE DocumentoPessoaId = %s"
        result = self.operator.mssql_conecta.fetch_custom_query_as_dicts(sql, (pessoa_conecta_id,))
        return result is not None and len(result) > 0

    def _transform(self, data, pessoa_conecta_id):
        """Transforma os dados do pesquisador no formato esperado pelo endpoint de documento."""
        try:
            data_emissao_str = data["pesquisador_data_emissao"][:19]
            data_emissao_iso = self.__format_date_to_iso(data_emissao_str)

            identificador = data.get("pesquisador_rg", "").strip()
            if re.search(r"[a-zA-Z]", identificador) or len(identificador) <= 4:
                identificador = data.get("pesquisador_cpf", "").strip()
            else:
                identificador = re.sub(r"\D", "", identificador).strip()

            return {
                "numero": identificador,
                "ufOrgaoEmissor": str(data["rg_uf"]).strip(),
                "orgaoEmissor": str(data["pesquisador_orgao_emissor"]).strip(),
                "dataEmissao": data_emissao_iso,
                "tipoDocumento": TipoDocumentoEnum.CARTEIRA_IDENTIDADE.value, 
                "pessoaId": pessoa_conecta_id
            }

        except Exception as e:
            self.operator.log.error("Erro de Transformação Documento: %s", str(e))
            raise e
        
    def execute(self, data, pessoa_conecta_id):
        
        if pessoa_conecta_id in self.documentos_processados:
            return
            
        if not self._check_exists(pessoa_conecta_id):
            payload = self._transform(data, pessoa_conecta_id)
            self.operator._sink_conecta(payload, 'documento')
            
            self.documentos_processados.add(pessoa_conecta_id)
            return
        
        self.documentos_processados.add(pessoa_conecta_id)
        self.operator.log.warning('Documento já existe do pesquisador %s', pessoa_conecta_id)