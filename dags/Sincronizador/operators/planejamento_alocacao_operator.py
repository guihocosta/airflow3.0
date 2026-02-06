from ..util.date_utils import DateUtils

class PlanejamentoAlocacaoOperator():

    def __init__(self, operator):
        self.operator = operator

    def _transform(self, raw_data, conecta_id):

        orcamento_bolsa = None
        for item in raw_data.get('orcamento_contratado').get('data'):
            if item.get('descricao_categoria') == 'Bolsas':
                orcamento_bolsa = float(item.get('valor_categoria'))
                break
        
        if orcamento_bolsa is None:
            self.operator.log.info("Orçamento de Bolsa não encontrado para o projeto: %s", raw_data.get('projeto_id'))
            orcamento_bolsa = float()
        
        return {
                "data": DateUtils.format_date_to_iso(raw_data.get('projeto_data_inicio_previsto')),
                "orcamentoBolsa": orcamento_bolsa,
                "projetoId": conecta_id,
                }

    def execute(self, raw_data, conecta_id):
        
        # Transform
        payload = self._transform(raw_data, conecta_id)

        # Load
        conecta_result = self.operator._sink_conecta(payload, 'planejamentoalocacao')

        return conecta_result

        