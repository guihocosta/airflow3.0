class NaturalidadeOperator():
    def __init__(self, operator):
        self.operator = operator
        self.naturalidades_processadas_cache = set()

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
        if pessoa_conecta_id in self.naturalidades_processadas_cache:
            self.operator.log.info("Naturalidade já processada nesta execução: %s", pessoa_conecta_id)
            return
            
        payload = self._transform(data, pessoa_conecta_id)
        self.operator._sink_conecta(payload, 'naturalidade')
        
        self.naturalidades_processadas_cache.add(pessoa_conecta_id)