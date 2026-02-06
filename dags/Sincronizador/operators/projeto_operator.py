from airflow.utils.decorators import apply_defaults
from concurrent.futures import ThreadPoolExecutor
from .fapes2conecta_operator import Fapes2ConectaOperator
from dateutil import parser
from datetime import timezone
from .planejamento_alocacao_operator import PlanejamentoAlocacaoOperator
from .planejamento_nivel_operator import PlanejamentoNivelOperator
from .coordenador_operator import CoordenadorOperator
from .pessoa_operator import PessoaOperator
import uuid
import datetime
from ..enums import EnumStatusProjeto

class ProjetoOperator(Fapes2ConectaOperator):

    TABLE_PROJECT_IMPORT_SQLSERVER = 'dbo.ProjectImport'
    
    COLUMNS_PROJECT_IMPORT_SQLSERVER = [
                "Id",
                "IdSigfapes",
                "Nome",
                "Status",
                "ProjetoConectaId",
                "EhProjetoImportado",
                "PossuiAlocacoesBolsistasImportados",
                "DateCreated",
                "DateUpdated",
                "DateDeleted",
                "AreaTecnica"
            ]
    
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, funcao="projetos", **kwargs)
        self.task_ids = 'Edital'
        self.key = 'editais'
        self.attrname='codedt'
        self.selected_id = 'projeto_id'
        self.selected_status = 'projeto_situacao'
        self.parent_id = 'edital_id'

        self.projeto_raw_data = {}
        self.planejamento_alocacao_operator = None
        self.planejamento_nivel_operator = None
        self.coordenador_operator = None
        self.pessoa_operator = None

    def __format_date_to_iso(self, date_str):
        dt_obj = parser.parse(date_str, dayfirst=True)
        dt_utc = dt_obj.astimezone(timezone.utc)
        return dt_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z')
  
    def _parse_status(self, status_sigfapes):
        """
        Converte o código de status SigFapes em EnumStatusProjeto.
        """
        try:
            code = int(status_sigfapes)
        except (ValueError, TypeError):
            return EnumStatusProjeto.INDEFINIDO
        if code == 8:
            return EnumStatusProjeto.EM_ANDAMENTO
        elif code == 9:
            return EnumStatusProjeto.FINALIZADO
        elif code == 10:
            return EnumStatusProjeto.CANCELADO
        elif code == 11:
            return EnumStatusProjeto.SUBSTITUIDO
        else:
            return EnumStatusProjeto.INDEFINIDO
    
    def _calc_orcamento_total(self, orcamento_contratado):
        total = 0

        if not orcamento_contratado:
            return total
        
        for item in orcamento_contratado:
            total += float(item.get('valor_categoria') or '0')

        return total
    
    def _prepare_record_sqlserver(self, data, conecta_id):
        return (
            uuid.uuid4(),  # Gera um novo UUID para o ID
            data["idSigfapes"],
            data["nome"],
            data["statusSigfapes"],
            conecta_id,
            True,        
            False,
            datetime.datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),        
            None,                      
            None,
            data["AreaTecnica"]
        )
         
    def _filter(self,data):

        # Busca IDs que já existem no banco
        existing_ids = self.mssql.fetch_existing_ids(
            table=self.TABLE_PROJECT_IMPORT_SQLSERVER,
            column="IdSigfapes",
            ids=data
        )

        novos_ids = [id for id in data if str(id) not in existing_ids]

        return novos_ids
   
    def _transform(self, data):

        projeto_transform = []

        for projeto in data:

            projeto["edital_id"] = self.parentSigfapesId

            try:
                
                dados_pessoais_coordenador = None

                titulo = projeto.get("projeto_titulo")
                if titulo is None:
                    self.log.error("Projeto %s sem título. Pulando para o próximo.", projeto.get("projeto_id"))
                    raise ValueError
                
                if self.area_tecnica != projeto.get("area_tecnica", None):
                    self.log.info("Áreas técnicas diferentes: %s %s", self.area_tecnica, projeto.get("area_tecnica", None))
                    continue

                status_enum = self._parse_status(projeto.get('projeto_situacao', ''))
                status_value = list(EnumStatusProjeto).index(status_enum)

                orc_projeto = projeto.get('orcamento_contratado', {}).get('data', [])
                
                if projeto.get('dados_coordenador').get('data'):
                    dados_pessoais_coordenador = projeto.get('dados_coordenador').get('data')[0]
                
                self.projeto_raw_data[str(projeto.get('projeto_id'))] = projeto

                projeto_transform.append(
                    {
                        "nome": projeto.get("projeto_titulo"),
                        "orcamentoTotal": self._calc_orcamento_total(orc_projeto),
                        "dataInicio": self.__format_date_to_iso(projeto.get("projeto_data_inicio_previsto")),
                        "dataFimPrevistaAtividade": self.__format_date_to_iso(projeto.get("projeto_data_fim_previsto")),
                        "idSigfapes": int(projeto.get("projeto_id")),
                        "statusProjeto": status_value,
                        "editalId": self.parentConectaId,
                        "statusSigfapes": projeto.get("projeto_situacao"),
                        "AreaTecnica":projeto.get("area_tecnica"),
                        'DadosCoordenador': dados_pessoais_coordenador
                    })
            except Exception as e:
                self.log.error("Erro de Transformação do Projeto: %s: %s", projeto.get("projeto_id"), str(e))
                raise e
        
        return projeto_transform

    def execute(self, context):
        if self.mssql is None:
            from ..util.sqlserver import SqlServer
            self.mssql = SqlServer(conn_id="sql_server_importacao")

        if self.planejamento_alocacao_operator is None:
            from .planejamento_alocacao_operator import PlanejamentoAlocacaoOperator
            self.planejamento_alocacao_operator = PlanejamentoAlocacaoOperator(self)

        if self.planejamento_nivel_operator is None:
            from .planejamento_nivel_operator import PlanejamentoNivelOperator
            self.planejamento_nivel_operator = PlanejamentoNivelOperator(self)

        if self.coordenador_operator is None:
            from .coordenador_operator import CoordenadorOperator
            self.coordenador_operator = CoordenadorOperator(self)

        if self.pessoa_operator is None:
            from .pessoa_operator import PessoaOperator
            self.pessoa_operator = PessoaOperator(self)

        # Load existing elements
        existing_elements = self.mssql.fetch_as_dicts(
            table=self.TABLE_PROJECT_IMPORT_SQLSERVER,
            columns=["IdSigfapes", "ProjetoConectaId", "Status", "AreaTecnica"]
        )

        if len(existing_elements) != 0:
            for elem in existing_elements:
                if int(elem["Status"]) == 8:
                    self.new_ids[str(elem["ProjetoConectaId"])] = (str(elem["IdSigfapes"]), elem.get("AreaTecnica", None))

        # Call parent execute
        return super().execute(context)

    def _sink(self, data_transform, data):
        data_importacao_sqlserver = []

        for i in range(len(data_transform)):
            try:
                request = data_transform[i].copy()
                del request['statusSigfapes']
                del request['AreaTecnica']
                del request['DadosCoordenador']
                
                _, conecta_result = self._sink_conecta(request, 'projeto')

                self.new_ids[str(conecta_result)] = (str(data_transform[i]['idSigfapes']), data_transform[i]['AreaTecnica'])

                # Preparar dados para Importação
                record_sqlserver = self._prepare_record_sqlserver(data_transform[i], conecta_result)
                data_importacao_sqlserver.append(record_sqlserver)

                if data_transform[i].get("DadosCoordenador"):
                    coordenador_pessoa_id, _ = self.pessoa_operator.execute(data_transform[i].get("DadosCoordenador"))
                    self.pessoa_operator.sink_importacao()
                    
                    request_coordenacao = {
                        "dataInicio": data_transform[i].get('dataInicio'),
                        "dataFim": data_transform[i].get('dataFimPrevistaAtividade'),
                        "pessoaId": str(coordenador_pessoa_id),
                        "projetoId": str(conecta_result)
                    }
                    self.coordenador_operator.execute(request_coordenacao)
                    self.log.info('Coordenador importado com sucesso!')

                # Importação de Planejamento de Alocação
                # if str(data_transform[i].get('idSigfapes')) in self.projeto_raw_data:
                #     projeto = self.projeto_raw_data[str(data_transform[i].get('idSigfapes'))]
                #     _, planejamento_result = self.planejamento_alocacao_operator.execute(raw_data=projeto, conecta_id=conecta_result)
                #     self.log.info("Planejamento de Alocação inserido com sucesso para o projeto: %s", planejamento_result)
                #     self.planejamento_nivel_operator.execute(projeto, planejamento_result)
                
            except Exception as e:
                self.log.error("Erro ao processar projeto %s: %s", data_transform[i].get('idSigfapes'), str(e))
                
                # Salvar projetos já processados antes de propagar a exceção
                if data_importacao_sqlserver:
                    self.log.info("Salvando %s projetos já processados antes de falhar", len(data_importacao_sqlserver))
                    self._sink_importacao(
                        table_sqlserver=self.TABLE_PROJECT_IMPORT_SQLSERVER,
                        values_sqlserver=data_importacao_sqlserver,
                        columns_sqlserver=self.COLUMNS_PROJECT_IMPORT_SQLSERVER
                    )
                    self.log.info("%s projetos salvos no banco de Importação antes da falha", len(data_importacao_sqlserver))
                
                # Limpar dados do projeto atual que falhou
                self.projeto_raw_data.clear()
                
                # Propagar exceção para que o Airflow saiba que houve falha
                raise e

        if len(data_importacao_sqlserver) != 0:
            self._sink_importacao(
                table_sqlserver=self.TABLE_PROJECT_IMPORT_SQLSERVER,
                values_sqlserver=data_importacao_sqlserver,
                columns_sqlserver=self.COLUMNS_PROJECT_IMPORT_SQLSERVER
            )
            self.projeto_raw_data.clear()
            self.log.info("%s projetos importados com sucesso.", len(data_importacao_sqlserver))

    def _filter_to_update(self, dict_sigfapes, parent_id):

        ids = list(dict_sigfapes.keys())

        data_conecta = self.mssql.fetch_as_dicts(
            table=self.TABLE_PROJECT_IMPORT_SQLSERVER,
            columns=["IdSigfapes", "ProjetoConectaId", "Status"],
            where_column='IdSigfapes',
            ids=ids
        )

        to_update = []

        for row in data_conecta:
            id_sig = row["IdSigfapes"]
            conecta_id = row["ProjetoConectaId"]

            if str(id_sig) not in dict_sigfapes:
                self.log.info('Leitura via arquivo: Projeto não encontrado para sync')
                continue

            status_atual = int(row["Status"])
            status_novo = int(dict_sigfapes.get(id_sig).get('status'))

            if status_novo is None:
                continue

            if status_novo != status_atual:
                to_update.append(
                    {
                        "id": conecta_id,
                        "idSigfapes":id_sig,
                        "statusProjeto" : status_novo
                    }
                )
        return to_update
        
    def _transform_update(self, data):

        projeto_transform_update = []

        for projeto in data:
            try:
                status_enum = self._parse_status(projeto.get("statusProjeto"))
                status_value = list(EnumStatusProjeto).index(status_enum)

                projeto_transform_update.append({
                    "id": str(projeto.get("id")),
                    "statusProjeto": status_value,
                    "statusSigfapes": projeto.get("statusProjeto"),
                    "idSigfapes": projeto.get('idSigfapes')
                })
            except Exception as e:
                self.log.error("Erro de Tranformação Update do Projeto: %s: %s", projeto.get("id"), str(e))
                raise e
        
        return projeto_transform_update
 
    def _sink_update(self, data):
        
        self.log.info("%s projetos a serem atualizados.", len(data))
        data_update_importacao = []

        for projeto in data:
            request = projeto.copy()
            del request["statusSigfapes"]
            projeto_conecta_result = self._sink_conecta_update(request, f'projeto/updateparcial/{projeto.get('id')}')
            
            data_update_importacao.append(
                {
                    'ProjetoConectaId':projeto.get('id'),
                    'Status': projeto.get('statusSigfapes')
                }
            )
        
        self._sink_importacao_update(self.TABLE_PROJECT_IMPORT_SQLSERVER, 'ProjetoConectaId', ['Status'], data_update_importacao)
        self.log.info("%s projetos atualizados com sucesso.", len(data_update_importacao))
