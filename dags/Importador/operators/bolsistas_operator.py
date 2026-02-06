from datetime import timezone
from dateutil.relativedelta import relativedelta
from concurrent.futures import ThreadPoolExecutor
from dateutil import parser
import uuid
import datetime
import pandas as pd
from .fapes2conecta_operator import Fapes2ConectaOperator
from ..enums import StatusAlocacaoBolsista
from ..util.sqlserver import SqlServer

class BolsistasOperator(Fapes2ConectaOperator):

    TABLE_ALOCACAO_IMPORT_SQLSERVER = 'dbo.AlocacaoBolsistaImport'
    COLUMNS_ALOCACAO_IMPORT_SQLSERVER = [
        "Id", "IdSigfapes", "Status", "Nome", "Cpf",
        "AlocacaoBolsistaConectaId", "EhAlocacaoBolsistaImportado",
        "PessoaConectaId", "DateCreated", "DateUpdated", "DateDeleted"
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, funcao="bolsistas", **kwargs)
        self.task_ids = 'Projeto'
        self.key = 'projetos'
        self.attrname = 'codprj'
        self.selected_id = 'formulario_bolsa_id'
        self.selected_status = 'formulario_bolsa_situacao'
        self.parent_id = 'projeto_id'
        self.with_dependency = True

        self.pessoa_operator = None
        self.naturalidade_operator = None
        self.documento_operator = None
        self.endereco_operator = None
        self.dadosbancarios_operator = None
    
    def _filter(self, data):
        existing_ids = self.mssql.fetch_existing_ids(
            table=self.TABLE_ALOCACAO_IMPORT_SQLSERVER,
            column="IdSigfapes",
            ids=data
        )

        novos_ids = [id for id in data if str(id) not in existing_ids]
        return novos_ids

    def __format_date_to_iso(self, date_str):
        dt_obj = parser.parse(date_str)
        dt_utc = dt_obj.astimezone(timezone.utc)
        return dt_utc.isoformat(timespec='milliseconds').replace('+00:00', 'Z')

    def _calc_qtd_cotas(self, inicio, termino):
        diff = relativedelta(termino, inicio)
        return diff.years * 12 + diff.months + 1

    def _get_last_pagamento(self, folhas, inicio):
        if not folhas:
            return inicio
        return max(parser.parse(f['folha_pagamento_data']) for f in folhas)

    def _has_reducao(self, nivel_nome):
        return "%" in nivel_nome if nivel_nome else False

    def _parse_status(self, situacao):
        mapping = {
            "1": StatusAlocacaoBolsista.EM_EDICAO,
            "2": StatusAlocacaoBolsista.EM_AVALIACAO,
            "3": StatusAlocacaoBolsista.EM_EDICAO,
            "7": StatusAlocacaoBolsista.REPROVADA,
            "8": StatusAlocacaoBolsista.ATIVA,
            "9": StatusAlocacaoBolsista.FINALIZADA,
            "10": StatusAlocacaoBolsista.SUSPENSA,
            "12": StatusAlocacaoBolsista.CANCELADA,
        }
        return mapping.get(situacao, StatusAlocacaoBolsista.INDEFINIDO)

    def _transform(self, data):

        data_transform = []

        for alocacao in data:

            alocacao["projeto_id"] = self.parentSigfapesId

            try:
                dt_inicio = parser.parse(alocacao["formulario_bolsa_inicio"])
                dt_termino = parser.parse(alocacao["formulario_bolsa_termino"])
                cancel = alocacao.get("formulario_cancel_bolsa_data_permanencia_bolsista")
                historico_pagamento = []
                historico_bolsas = []

                for pagamento in alocacao.get("folha_pagamentos", []):
                    historico_pagamento.append(
                        {
                            "mesCompetencia": self.__format_date_to_iso(pagamento["folha_pagamento_data"]),
                            "dataPagamento": self.__format_date_to_iso(pagamento["folha_pagamento_gerada"]),
                            "valorPago": float(pagamento["folha_pagamento_pesquisador_valor"]),
                            "alocacaoId": str(uuid.uuid4())
                        }
                    )
                
                for bolsa in alocacao.get("historicoBolsas", []):
                    historico_bolsas.append(
                        {
                            "dataInicio": bolsa.get('dataInicio'),
                            "dataFim": bolsa.get('dataFim'),
                            "valorBolsa": float(alocacao.get('bolsa_nivel_valor', "0")),
                            "modalidadeBolsa": alocacao.get('bolsa_sigla', ''),
                            "possuiReducaoBolsa": '%' in (alocacao.get('bolsa_nivel_nome') or "")
                        }
                    )

                dados_pessoais = alocacao.get("dados_pessoais", {})
                data_transform.append(
                    {
                        "statusSigfapes": alocacao.get("formulario_bolsa_situacao", ""),
                        "nome": dados_pessoais.get("pesquisador_nome", ""),
                        "cpf": dados_pessoais.get("pesquisador_cpf", "" ),
                        "token": "",
                        "dataInicio": self.__format_date_to_iso(alocacao["formulario_bolsa_inicio"]),
                        "dataPrevistaFimAtividade": self.__format_date_to_iso(alocacao["formulario_bolsa_termino"]),
                        "dataFimAtividade": self.__format_date_to_iso(cancel) if cancel else None,
                        "qtdeCotasAlocadas": self._calc_qtd_cotas(dt_inicio, dt_termino),
                        "qtdeCotasPagasPreImportacao": len(alocacao.get("folha_pagamentos", [])),
                        "status": self._parse_status(alocacao["formulario_bolsa_situacao"]).value,
                        "idSigfapes": int(alocacao["formulario_bolsa_id"]),
                        "pessoaId": str(alocacao["pessoaId"]),
                        "projetoId": self.parentConectaId,
                        "valorBolsa": float(alocacao.get("bolsa_nivel_valor")),
                        "modalidadeBolsa": alocacao.get("bolsa_sigla", ""),
                        "possuiReducaoBolsa": self._has_reducao(alocacao.get("bolsa_nivel_nome", "")),
                        "dataUltimoPagamento": self.__format_date_to_iso(
                            self._get_last_pagamento(
                                alocacao.get("folha_pagamentos", []), dt_inicio
                            ).isoformat()
                        ),
                        "historicoPagamento": historico_pagamento,
                        "historicoBolsas":historico_bolsas
                    })
            except Exception as e:
                self.log.error("Erro de Transformação de Alocação: (%s): %s", alocacao.get("formulario_bolsa_id"), str(e))
                raise e
        
        return data_transform

    def _import_dependency(self, data):

        for alocacao in data:
            try:
                dados = alocacao.get("dados_pessoais")
                pesquisador_id_sigfapes = alocacao.get("bolsista_pesquisador_id")
                pessoa_conecta_id, _ = self.pessoa_operator.execute(dados)

                pessoa_conecta_id = str(pessoa_conecta_id)

                alocacao["pessoaId"] = pessoa_conecta_id

                try:
                    self.naturalidade_operator.execute(dados, pessoa_conecta_id)
                    self.documento_operator.execute(dados, pessoa_conecta_id)
                    self.endereco_operator.execute(dados, pessoa_conecta_id)
                    self.dadosbancarios_operator.execute(alocacao, pessoa_conecta_id)
                except Exception as e:
                    self.log.warning(str(e))
                    continue

            except Exception as e:
                self.log.error("Erro ao importar dependências da alocação %s: %s", alocacao.get("formulario_bolsa_id"), str(e))
                self.pessoa_operator.sink_importacao()
                raise e

        self.pessoa_operator.sink_importacao()
        return data

    def __prepare_record_sql(self, data_transform,aloc_con_id):
        return (
            uuid.uuid4(),
            data_transform.get('idSigfapes'),
            data_transform.get('statusSigfapes'),
            data_transform.get('nome'),
            data_transform.get('cpf'),
            str(aloc_con_id),
            True,
            data_transform.get('pessoaId'),
            datetime.datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z'),
            None,
            None
        )

    def execute(self, context):
        if self.mssql is None:
            from ..util.sqlserver import SqlServer
            self.mssql = SqlServer(conn_id="sql_server_importacao")

        if self.pessoa_operator is None:
            from .pessoa_operator import PessoaOperator
            self.pessoa_operator = PessoaOperator(self)

        if self.naturalidade_operator is None:
            from .naturalidade_operator import NaturalidadeOperator
            self.naturalidade_operator = NaturalidadeOperator(self)

        if self.documento_operator is None:
            from .documento_operator import DocumentoOperator
            self.documento_operator = DocumentoOperator(self)

        if self.endereco_operator is None:
            from .endereco_operator import EnderecoOperator
            self.endereco_operator = EnderecoOperator(self)

        if self.dadosbancarios_operator is None:
            from .dadosbancarios_operator import DadosBancariosOperator
            self.dadosbancarios_operator = DadosBancariosOperator(self)

        return super().execute(context)
    def _sink(self, data_transform, data):

        data_importacao_sqlserver = []

        for alocacao in data_transform:
            try:
                payload = alocacao.copy()
                del payload["nome"]
                del payload["cpf"]
                del payload["statusSigfapes"]
                _, alocacao_result = self._sink_conecta(payload, 'alocacaobolsista/import')

                data_importacao_sqlserver.append(self.__prepare_record_sql(alocacao, alocacao_result))

            except Exception as e:
                self.log.error("Erro ao processar alocação %s: %s", alocacao.get('idSigfapes'), str(e))
                
                if data_importacao_sqlserver:
                    self.log.info("Salvando %s alocações já processadas antes de falhar", len(data_importacao_sqlserver))
                    self._sink_importacao(
                        table_sqlserver=self.TABLE_ALOCACAO_IMPORT_SQLSERVER,
                        values_sqlserver=data_importacao_sqlserver,
                        columns_sqlserver=self.COLUMNS_ALOCACAO_IMPORT_SQLSERVER
                    )
                    self.log.info("%s alocações salvas no banco de Importação antes da falha", len(data_importacao_sqlserver))
                
                raise e

        if len(data_importacao_sqlserver) != 0:
            self._sink_importacao(
                table_sqlserver=self.TABLE_ALOCACAO_IMPORT_SQLSERVER,
                values_sqlserver=data_importacao_sqlserver,
                columns_sqlserver=self.COLUMNS_ALOCACAO_IMPORT_SQLSERVER
            )
            self.log.info("%s novas alocações encontradas e inseridas.", len(data_importacao_sqlserver))
    
    def _filter_to_update(self, dict_sigfapes, parent_id):

        ids = list(dict_sigfapes.keys())
        if not ids:
            return []

        data_importacao = self.mssql.fetch_as_dicts(
            table=self.TABLE_ALOCACAO_IMPORT_SQLSERVER,
            columns=["IdSigfapes", "AlocacaoBolsistaConectaId", "Status"],
            where_column='IdSigfapes',
            ids=ids
        )

        response_conecta = self.get_conecta(f'alocacaobolsista/visualizarbolsistas/projeto/{parent_id}')

        data_alocacoes_conecta = response_conecta.get('value', {}).get('alocacoes', [])

        if data_alocacoes_conecta is None:
            data_alocacoes_conecta = []
        
        dict_conecta = {
            item['id']: item
            for item in data_alocacoes_conecta
        }

        to_update = []
        
        for sql_row in data_importacao:
            raw_uuid = sql_row.get("AlocacaoBolsistaConectaId")
            if not raw_uuid: continue
            
            conecta_id = str(raw_uuid).lower()
            sigfapes_id = sql_row.get('IdSigfapes')

            sigfapes_data = dict_sigfapes.get(sigfapes_id)
            conecta_data = dict_conecta.get(conecta_id)

            if not self.__validar_existencia(sigfapes_id, conecta_id, sigfapes_data, conecta_data):
                continue

            status_sql = int(sql_row.get("Status"))
            status_sigfapes = int(sigfapes_data.get('status'))
            
            data_fim_conecta = conecta_data.get("dataFimPrevistaAtividade")
            data_fim_sigfapes = sigfapes_data.get('raw', {}).get('formulario_bolsa_termino')

            precisa_atualizar, diff_log = self.__verificar_divergencia(
                status_sql, status_sigfapes, 
                data_fim_conecta, data_fim_sigfapes
            )

            if precisa_atualizar:
                self.log.info(f"DIVERGÊNCIA encontrada ID {sigfapes_id}: {diff_log}")
                self.log.info(sigfapes_data)
                
                dados_cancelamento = self.__processar_regras_cancelamento(sigfapes_data, sigfapes_id)
                
                if dados_cancelamento['ignorar_update']:
                    continue

                payload = {
                    "id": conecta_id,
                    "status": status_sigfapes,
                    "idSigfapes": sigfapes_id,
                    "dataFimAtividade": dados_cancelamento['data_fim_formatada'],
                    "dataPrevistaFimAtividade": pd.to_datetime(data_fim_sigfapes, utc=True).isoformat()
                }
                to_update.append(payload)

        return to_update

    def __validar_existencia(self, sig_id, conecta_id, sig_data, con_data):
        """Valida se os dados existem nas duas pontas antes de processar."""
        if not sig_data:
            self.log.warning(f'Bolsista {sig_id} no SQL mas não no dict Sigfapes. Ignorando.')
            return False
        if not con_data:
            self.log.warning(f'Bolsista {sig_id} ({conecta_id}) no SQL mas não na API Conecta. Ignorando.')
            return False
        return True

    def __verificar_divergencia(self, s_atual, s_novo, d_atual_str, d_nova_str):
        """Retorna True se houver diferença, junto com o motivo (log)."""
        reasons = []
        
        if s_atual != s_novo:
            reasons.append(f"Status: {s_atual} -> {s_novo}")

        dt_atual = pd.to_datetime(d_atual_str, utc=True)
        dt_nova = pd.to_datetime(d_nova_str, utc=True)

        datas_diferentes = False

        ambos_nulos = pd.isna(dt_atual) and pd.isna(dt_nova)
        um_nulo = pd.isna(dt_atual) != pd.isna(dt_nova)

        if ambos_nulos:
            datas_diferentes = False
        elif um_nulo:
            datas_diferentes = True
            reasons.append(f"Data Nula vs Preenchida: {dt_atual} -> {dt_nova}")
        else:
            if dt_nova.date() > dt_atual.date():
                datas_diferentes = True
                reasons.append(f"Data: {dt_atual.date()} -> {dt_nova.date()}")

        return (len(reasons) > 0), "; ".join(reasons)

    def __processar_regras_cancelamento(self, sigfapes_data, sig_id):
        """Aplica a regra de negócio do dia 16 para cancelamentos."""
        DIA_CORTE = 16
        hoje = datetime.datetime.now(timezone.utc)
        
        raw_data = sigfapes_data.get('raw', {})
        dt_cancel_str = raw_data.get("formulario_cancel_bolsa_data_permanencia_bolsista")
        
        resultado = {
            'ignorar_update': False,
            'data_fim_formatada': None
        }

        if not dt_cancel_str:
            return resultado

        try:
            dt_cancel = parser.parse(dt_cancel_str)
            if dt_cancel.tzinfo is None:
                dt_cancel = dt_cancel.replace(tzinfo=timezone.utc)
            
            resultado['data_fim_formatada'] = self.__format_date_to_iso(dt_cancel_str)

            mesmo_mes = (dt_cancel.year == hoje.year) and (dt_cancel.month == hoje.month)
            
            if mesmo_mes:
                if dt_cancel.day >= DIA_CORTE:
                    self.log.info(f"Bolsista {sig_id}: Cancelamento dia {dt_cancel.day} (>=16). Postergar corte para próximo mês.")
                    resultado['ignorar_update'] = True
                else:
                    self.log.info(f"Bolsista {sig_id}: Cancelamento dia {dt_cancel.day} (<16). Processar corte imediato.")
            
        except Exception as e:
            self.log.error(f"Erro ao processar data cancelamento {sig_id}: {e}")
            pass

        return resultado

    def _transform_update(self, data):

        alocacao_transform_update = []

        for alocacao in data:
            try:
            
                status_sigfapes_str = str(alocacao.get("status"))

                status_enum = self._parse_status(status_sigfapes_str)

                status_value = status_enum.value

                alocacao_transform_update.append({
                    "id": str(alocacao.get("id")),
                    "status": status_value,
                    "idSigfapes": alocacao.get('idSigfapes'),
                    'statusSigfapes': alocacao.get("status") ,
                    "dataFimAtividade": alocacao.get("dataFimAtividade"),
                    "dataPrevistaFimAtividade": alocacao.get("dataPrevistaFimAtividade")
                })
            except Exception as e:
                self.log.error("Erro de Transformação Update de Alocação: (%s): %s", alocacao.get('id'), str(e))
                raise e
        
        return alocacao_transform_update
        
    def _sink_update(self, data):
        
        self.log.info("%s alocações serão atualizadas.", len(data))
        data_update_importacao = []

        for alocacao in data:
            request = alocacao.copy()
            del request["statusSigfapes"]
            alocacao_conecta_result = self._sink_conecta_update(request, f'alocacaobolsista/updateparcial/{alocacao.get('id')}')
            
            data_update_importacao.append(
                {
                    'AlocacaoBolsistaConectaId':str(alocacao.get('id')),
                    'Status': alocacao.get('statusSigfapes')
                }
            )
        
        self.log.info("%s alocações atualizadas com sucesso.", len(data_update_importacao))
        self._sink_importacao_update(self.TABLE_ALOCACAO_IMPORT_SQLSERVER, 'AlocacaoBolsistaConectaId', ['Status'], data_update_importacao)
    