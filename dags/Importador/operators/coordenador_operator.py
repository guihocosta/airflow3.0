class CoordenadorOperator():

    def __init__(self, operator):
        self.operator = operator

    def _transform(self, coordenador_data):
        return {        
            "dataInicio": coordenador_data.get('dataInicio'),
            "dataFim": coordenador_data.get('dataFim'),
            "pessoaId": coordenador_data.get('pessoaId'),
            "projetoId": coordenador_data.get('projetoId')
        }

    def execute(self, coordenador_data):
        
        # Transform
        payload = self._transform(coordenador_data)

        # Load
        self.operator._sink_conecta(payload, 'coordenacao')

        