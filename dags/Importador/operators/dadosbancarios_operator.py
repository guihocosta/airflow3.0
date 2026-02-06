import re

class DadosBancariosOperator():

    def __init__(self, operator):
        self.operator = operator
        self.dados_bancarios_processados = set()
    
    def _transform(self, data, pessoa_conecta_id):
        """Transforma os dados bancários no formato esperado pelo endpoint."""
        try:
            conta = re.sub(r"\D", "", data.get("formulario_numero_conta","")).strip()
            agencia = re.sub(r"\D", "", data.get("formulario_numero_agencia", "")).strip()

            if conta == "" or agencia == "":
                self.operator.log.warning("Dados bancários incompletos")
                conta = '000'
                agencia = '000'
            
            return {
                "conta": conta,
                "agencia": agencia,
                "pessoaId": pessoa_conecta_id,
                "bancoId": "2FEFF1B0-5298-491E-85F1-ABAD4C3B72AD"
            }

        except Exception as e:
            self.operator.log.error("Erro de Transformação Dados Bancários: %s", str(e))
            raise e

    def execute(self, data, pessoa_conecta_id):

        if pessoa_conecta_id in self.dados_bancarios_processados:
            self.operator.log.warning('Dados Bancários já existem do pesquisador %s', pessoa_conecta_id)
            return

        payload = self._transform(data, pessoa_conecta_id)
        self.operator._sink_conecta(payload, 'dadosbancarios')
        
        self.dados_bancarios_processados.add(pessoa_conecta_id)
        return
