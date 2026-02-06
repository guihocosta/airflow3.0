from collections import defaultdict

class PlanejamentoNivelOperator():

    def __init__(self, operator):
        self.operator = operator
    
    def _transform(self, quantidade,  planejamento_alocacao_id, sigla, valorBolsa):
        
        try:
            valor_convertido = float(valorBolsa)

            return {
                        "quantidade": quantidade,
                        "planejamentoAlocacaoId": str(planejamento_alocacao_id),
                        "modalidadeBolsa": sigla,
                        "valorBolsa": valor_convertido
                    }
        except Exception as e:
            self.operator.log.error(
                "Erro ao transformar dados para Nível %s (Valor: %s). Erro: %s", 
                sigla, valorBolsa, e
            )
            return None

    def execute(self, raw_data, planejamento_alocacao_id):

        cotas_por_nivel = defaultdict(int)

        for grupo in raw_data.get('quadroBolsas', {}).get('data', []):

            sigla = grupo.get('sigla')
            orcamento_custo = grupo.get('orcamento_custo')

            if not sigla or orcamento_custo is None:
                self.operator.log.warning("Campos de Sigla e Orçamento deste Nível estão vazios/nulos, impedindo a importação deste registro de PlanejamentoNivel.")
                continue

            try:
                cotas = int(grupo.get('cotas', 0))

            except ValueError:
                self.operator.log.error("Cota inválida em grupo: %s", grupo)
                continue

            cotas_por_nivel[(sigla, orcamento_custo)] += cotas

        # Transform e Sink
        for (sigla, orcamento_custo), total_cotas in cotas_por_nivel.items():

            if total_cotas <= 0:
                self.operator.log.warning(
                    "Nível %s - %s NÃO enviado. Total de cotas é %s (Regra de Negócio: deve ser > 0).", 
                    sigla, orcamento_custo, total_cotas
                )
                continue
            
            payload = self._transform(total_cotas, planejamento_alocacao_id, sigla, orcamento_custo)
            if payload:
                self.operator._sink_conecta(payload, 'planejamentonivel/import')


        