class NaturalidadeOperator():
    def __init__(self, operator):
        self.operator = operator
        self.naturalidades_processadas_cache = set()

    def _check_exists(self, pessoa_conecta_id):
        """
        Verifica se os dados de Naturalidade já existem no banco.
        """
        sql = (
            "SELECT TOP 1 1 "
            "FROM dbo.Naturalidade "
            "WHERE NaturalidadePessoaId = %s"
        )
        self.operator.log.info("SQL: %s | PARAM: %s", sql, pessoa_conecta_id)
        
        result = self.operator.mssql_conecta.fetch_custom_query_as_dicts(
            sql,
            [pessoa_conecta_id]
        )
        self.operator.log.info("Result: %s", result)
        
        return bool(result)

    def _transform(self, data, pessoa_conecta_id):
        try:
            return {
                "cidade": str(data["municipio_nome"]).strip(),
                "uf": str(data["estado_uf"]).strip(),
                "pessoaId": pessoa_conecta_id
            }
        except Exception as e:
            self.operator.log.error("Erro de Transformação: %s", str(e))
            raise e

    def execute(self, data, pessoa_conecta_id):
        # Se já processamos, ignora
        if pessoa_conecta_id in self.naturalidades_processadas_cache:
            self.operator.log.debug("Naturalidade já processada nesta execução: %s", pessoa_conecta_id)
            return
        
        # Verifica se existe no banco
        if self._check_exists(pessoa_conecta_id):
            self.operator.log.warning("Naturalidade já existe no banco para pesquisador %s", pessoa_conecta_id)
            # IMPORTANTE: Adiciona ao cache para não verificar novamente
            self.naturalidades_processadas_cache.add(pessoa_conecta_id)
            return
        
        # Não existe - insere
        self.operator.log.info("Naturalidade não existe, inserindo para: %s", pessoa_conecta_id)
        payload = self._transform(data, pessoa_conecta_id)
        self.operator._sink_conecta(payload, 'naturalidade')
        
        # Adiciona ao cache após inserir
        self.naturalidades_processadas_cache.add(pessoa_conecta_id)