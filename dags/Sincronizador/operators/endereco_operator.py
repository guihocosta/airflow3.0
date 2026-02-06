class EnderecoOperator():

    def __init__(self, operator):
        self.operator = operator
        self.enderecos_processados = set()
    
    def _check_exists(self, pessoa_conecta_id):
        """
        Verifica se os dados de Endereço já existem.
        """
        sql = "SELECT TOP 1 1 FROM dbo.Endereco WHERE EnderecoPessoaId = %s"
        result = self.operator.mssql_conecta.fetch_custom_query_as_dicts(sql, (pessoa_conecta_id,))
        return result is not None and len(result) > 0

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
            return

        if not self._check_exists(pessoa_conecta_id):
            payload = self._transform(data, pessoa_conecta_id)
            self.operator._sink_conecta(payload, 'endereco')
            
            self.enderecos_processados.add(pessoa_conecta_id)
            return
        
        self.enderecos_processados.add(pessoa_conecta_id)
        self.operator.log.warning('Endereço já existe do pesquisador %s', pessoa_conecta_id)