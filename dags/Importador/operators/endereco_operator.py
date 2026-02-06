class EnderecoOperator():

    def __init__(self, operator):
        self.operator = operator
        self.enderecos_processados = set()
    
    def _transform(self, data, pessoa_conecta_id):
        try:
            return {
                "logradouro": str(data["endereco_rua"]).strip(),
                "numero": str(data["endereco_numero"]).strip(),
                "complemento": str(data.get("endereco_complemento", "")).strip(),
                "cep": str(data["endereco_cep"]).strip(),
                "bairro": str(data["endereco_bairro"]).strip(),
                "municipio": str(data["municipio_nome"]).strip(),
                "ufLocalidade": str(data["estado_uf"]).strip(),
                "pessoaId": pessoa_conecta_id
            }
        except Exception as e:
            self.operator.log.error("Erro de Transformação Endereço: %s", str(e))
            raise e

    def execute(self, data, pessoa_conecta_id):

        if pessoa_conecta_id in self.enderecos_processados:
            self.operator.log.warning('Endereço já existe do pesquisador %s', pessoa_conecta_id)
            return

        payload = self._transform(data, pessoa_conecta_id)
        self.operator._sink_conecta(payload, 'endereco')
        
        self.enderecos_processados.add(pessoa_conecta_id)
        return
